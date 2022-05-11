package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.hash.Hashing;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.hash.Hashing.consistentHash;
import static io.hops.transaction.context.EntityContext.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * Used by Serverless NameNodes to store and retrieve cached metadata.
 *
 * We partition the namespace by parent INode ID (and/or possibly the full path to the parent directory).
 * In this way, we basically map terminal INodes (i.e., files) to particular directories. Single INode operations
 * targeting these INodes will issue INVs to the deployment responsible for caching that terminal INode (i.e., file).
 * This will just invalidate that terminal INode, leaving the rest of the path intact.
 * If a subtree operation is performed, then a subtree/prefix INV will be issued to all NNs responsible for
 * caching some terminal INode in the subtree. This will invalidate all INodes along the path, ensuring that no
 * deployments will serve stale metadata.
 */
public class InMemoryINodeCache {
    public static final Logger LOG = LoggerFactory.getLogger(InMemoryINodeCache.class);

    /**
     * Default load factor used for the INode ID to full path cache.
     */
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Used to synchronize access to the PatriciaTrie {@code prefixMetadataCache} variable.
     * Multiple threads can access the PatriciaTrie concurrently, so we must synchronize access.
     * We use a RW lock since much of the time, the threads will just be concurrently reading.
     */
    private final ReadWriteLock _mutex = new ReentrantReadWriteLock(true);

    /**
     * This is the main cache, along with the cache HashMap.
     *
     * We use this object when we want to grab a bunch of INodes using a path prefix (e.g., /home/ben/docs/).
     */
    private final PatriciaTrie<INode> prefixMetadataCache;

    /**
     * Used to control the size of the trie structure. This has a capacity, and entries are automatically
     * evicted when we're at-capacity. We can use the eviction listener to then remove those entries
     * from the trie data structure and the other caches (e.g., ID to full path, parent ID + local name, etc.)
     */
    private final Cache<String, INode> cache;

//    /**
//     * Cache that is used when not using a prefix.
//     */
//    private final ConcurrentHashMap<String, INode> fullPathMetadataCache;

    /**
     * Mapping between INode IDs and their fully-qualified paths.
     */
    private final ConcurrentHashMap<Long, String> idToFullPathMap;

    /**
     * Mapping between keys of the form [PARENT_ID][LOCAL_NAME], which is how some INodes are
     * cached/stored by HopsFS during transactions, to the fully-qualified paths of the INode.
     */
    private final ConcurrentHashMap<String, String> parentIdPlusLocalNameToFullPathMapping;

    private final ThreadLocal<Integer> threadLocalCacheHits = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Integer> threadLocalCacheMisses = ThreadLocal.withInitial(() -> 0);

    private final boolean enabled;

    private final int deploymentNumber;

    private final int numDeployments;

    /**
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public InMemoryINodeCache(Configuration conf, int deploymentNumber) {
        this(conf, DEFAULT_LOAD_FACTOR, deploymentNumber);
    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public InMemoryINodeCache(Configuration conf, float loadFactor, int deploymentNumber) {
        //this.invalidatedKeys = new HashSet<>();
        /**
         * Maximum elements in INode cache.
         */
        int cacheCapacity = conf.getInt(SERVERLESS_METADATA_CACHE_CAPACITY, SERVERLESS_METADATA_CACHE_CAPACITY_DEFAULT);
        this.numDeployments = conf.getInt(SERVERLESS_MAX_DEPLOYMENTS, SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);
        this.idToFullPathMap = new ConcurrentHashMap<>(cacheCapacity, loadFactor);
        this.parentIdPlusLocalNameToFullPathMapping = new ConcurrentHashMap<>(cacheCapacity, loadFactor);
        this.deploymentNumber = deploymentNumber;

        // this.fullPathMetadataCache = new ConcurrentHashMap<>(capacity, loadFactor);

        /**
         * This is the main cache, along with the metadataTrie variable. We use this when we want to grab a single
         * INode by its full path.
         */
        this.prefixMetadataCache = new PatriciaTrie<>();
        this.cache = Caffeine.newBuilder()
                .initialCapacity(cacheCapacity)
                .evictionListener((RemovalListener<String, INode>) (fullPath, iNode, removalCause) -> {
                    _mutex.writeLock().lock();
                    try {
                        prefixMetadataCache.remove(fullPath);
                    } finally {
                        _mutex.writeLock().unlock();
                    }

//                    if (fullPath != null)
//                        fullPathMetadataCache.remove(fullPath);

                    if (iNode != null)
                        idToFullPathMap.remove(iNode.getId());
                })
                .build();
        this.enabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_METADATA_CACHE_ENABLED,
                DFSConfigKeys.SERVERLESS_METADATA_CACHE_ENABLED_DEFAULT);
    }

    /**
     * Set the 'cache hits' and 'cache misses' counters to 0.
     */
    public void resetCacheHitMissCounters() {
        threadLocalCacheHits.set(0);
        threadLocalCacheMisses.set(0);
    }

    /**
     * Record a cache hit.
     */
    protected void cacheHit(String path) {
        int currentHits = threadLocalCacheHits.get();
        threadLocalCacheHits.set(currentHits + 1);

        if (LOG.isTraceEnabled()) {
            LOG.trace(ANSI_GREEN + "[CACHE HIT]" + ANSI_RESET + " INode = '" + path + "'");
        }
    }

    /**
     * Record a cache miss.
     */
    protected void cacheMiss(String path) {
        int currentMisses = threadLocalCacheMisses.get();
        threadLocalCacheMisses.set(currentMisses + 1);

        if (LOG.isTraceEnabled()) LOG.trace(ANSI_RED + "[CACHE MISS]" + ANSI_RESET + " INode path = '" + path + "'");
    }

    /**
     * Record a cache miss.
     */
    protected void cacheMiss(String localName, long parentId) {
        int currentMisses = threadLocalCacheMisses.get();
        threadLocalCacheMisses.set(currentMisses + 1);
        if (LOG.isTraceEnabled()) LOG.trace(ANSI_RED + "[CACHE MISS]" + ANSI_RESET + " INode LocalName = '" +
                localName + "', ParentID = " + parentId);
    }

    /**
     * Record a cache hit.
     */
    protected void cacheMiss(long id) {
        int currentHits = threadLocalCacheHits.get();
        threadLocalCacheHits.set(currentHits + 1);

        if (LOG.isTraceEnabled())
            LOG.trace(ANSI_RED + "[CACHE MISS]" + ANSI_RESET + " INode ID = " + id);
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param key The fully-qualified path of the desired INode
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public INode getByPath(String key) {
        if (!enabled)
            return null;

        long s = System.currentTimeMillis();

        try {
            // INode returnValue = fullPathMetadataCache.get(key);
            INode returnValue = cache.getIfPresent(key);

            if (returnValue == null)
                cacheMiss(key);
            else
                cacheHit(key);

            return returnValue;
        } finally {
            long t4 = System.currentTimeMillis();
            if (LOG.isTraceEnabled() && t4 - s > 10) LOG.trace("Checked cache by path for INode '" + key + "' in " +
                    (t4 - s) + " ms. [1]");
        }
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param parentId The INode ID of the parent INode of the desired INode.
     * @param localName The local name of the desired INode.
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public INode getByParentINodeIdAndLocalName(long parentId, String localName) {
        if (!enabled)
            return null;

        long s = System.currentTimeMillis();
        try {
            String parentIdPlusLocalName = parentId + localName;
            String key = parentIdPlusLocalNameToFullPathMapping.getOrDefault(parentIdPlusLocalName, null);

            if (key != null)
                return getByPath(key);

            cacheMiss(localName, parentId);
            return null;
        } finally {
            if (LOG.isTraceEnabled()) {
                long t = System.currentTimeMillis();
                LOG.trace("Checked cache by parent ID and local name for INode '" + localName + "' in " + (t - s) + " ms.");
            }
        }
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param iNodeId The INode ID of the given metadata object.
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public INode getByINodeId(long iNodeId) {
        if (!enabled)
            return null;

        long s = System.currentTimeMillis();
        try {
            if (idToFullPathMap.containsKey(iNodeId)) {
                String key = idToFullPathMap.get(iNodeId);
                return getByPath(key);
            }

            cacheMiss(iNodeId);
            return null;
        } finally {
            if (LOG.isTraceEnabled()) {
                long t = System.currentTimeMillis();
                LOG.trace("Checked cache by ID for INode " + iNodeId + " in " + (t - s) + " ms.");
            }
        }
    }

    /**
     * Override `put()` to disallow null values from being added to the cache.
     *
     * @param key The fully-qualified path of the desired INode
     * @param iNodeId The INode ID of the given metadata object.
     * @param value The metadata object to cache under the given key.
     *
     * @return The previous value associated with key, or null if there was no mapping for key.
     */
    public INode put(String key, long iNodeId, INode value) {
        if (value == null)
            throw new IllegalArgumentException("INode Metadata Cache does NOT support null values. Associated key: " + key);
        long s = System.currentTimeMillis();

//        if (consistentHash(Hashing.md5().hashString(getPathToCache(key)), numDeployments) != deploymentNumber) {
//            if (LOG.isTraceEnabled()) {
//                long t = System.currentTimeMillis();
//                LOG.trace("Rejected INode '" + key + "' (ID=" + iNodeId + ") from being cached in " + (t - s) + " ms.");
//            }
//            return null;
//        }

        _mutex.writeLock().lock();
        INode returnValue;
        try {
            // Store the metadata in the cache directly.
            returnValue = prefixMetadataCache.put(key, value);
        } finally {
            _mutex.writeLock().unlock();
            if (LOG.isTraceEnabled()) {
                long t = System.currentTimeMillis();
                LOG.trace("Stored INode '" + key + "' (ID=" + iNodeId + ") in cache in " + (t - s) + " ms.");
            }
        }

        String parentIdPlusLocalName = value.getParentId() + value.getLocalName();
        parentIdPlusLocalNameToFullPathMapping.put(parentIdPlusLocalName, key);

        // Put into the Caffeine cache. We use this to implement the LRU policy.
        cache.put(key, value);

        // Create a mapping between the INode ID and the path.
        idToFullPathMap.put(iNodeId, key);

        // Cache by full-path.
        // fullPathMetadataCache.put(key, value);

        return returnValue;
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(Object key) {
        if (key == null)
            return false;

        _mutex.readLock().lock();
        try {
            // If the given key is a string, then we can use it directly.
            if (key instanceof String) {
                return prefixMetadataCache.containsKey(key);
            } else if (key instanceof Long) {
                // If the key is a long, we need to check if we've mapped this long to a String key. If so,
                // then we can get the string version and continue as before.
                String keyAsStr = idToFullPathMap.getOrDefault((Long) key, null);
                return keyAsStr != null && prefixMetadataCache.containsKey(keyAsStr);
            }

            return false;
        } finally {
            _mutex.readLock().unlock();
        }
    }

    /**
     * Return the size of the cache.
     */
    public int size() {
        _mutex.readLock().lock();
        try {
            return prefixMetadataCache.size();
        } finally {
            _mutex.readLock().unlock();
        }
    }

    /**
     * Check if there is an entry for the given key in this cache, regardless of whether not that entry is
     * marked as invalid or not. That is, this "skips" the check of whether this key is invalid.
     *
     * The regular containsKey() function only returns true if this contains the key AND the key has not been
     * invalidated.
     */
    public boolean containsKeySkipInvalidCheck(String key) {
        _mutex.readLock().lock();
        try {
            // Directly check if the cache itself contains the key.
            return prefixMetadataCache.containsKey(key);
        } finally {
            _mutex.readLock().unlock();
        }
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(long inodeId) {
        // If the key is a long, we need to check if we've mapped this long to a String key. If so,
        // then we can get the string version and continue as before.
        _mutex.readLock().lock();
        try {
            // If we don't have a mapping for this key, then this will return null, and this function will return false.
            String keyAsStr = idToFullPathMap.getOrDefault(inodeId, null);

            // Returns true if we are able to resolve the NameNode ID to a string-typed key, that key is not
            // invalidated, and we're actively caching the key.
            return keyAsStr != null && prefixMetadataCache.containsKey(keyAsStr);
        } finally {
            _mutex.readLock().unlock();
        }
    }

    /**
     * Invalidate a given key by passing the INode ID of the INode to be invalidated, rather than the
     * fully-qualified path.
     * @param inodeId The INode ID of the INode to be invalidated.
     *
     * @return True if the key was invalidated, otherwise false.
     */
    protected boolean invalidateKey(long inodeId) {
        _mutex.writeLock().lock();
        try  {
            if (idToFullPathMap.containsKey(inodeId)) {
                String key = idToFullPathMap.get(inodeId);

                return invalidateKeyInternal(key, false);
            }

            return false;
        } finally {
            _mutex.writeLock().unlock();
        }
    }

    /**
     * Return the fully-qualified path of the INode with the given ID, if we have a mapping for it.
     *
     * This mapping is not guaranteed to be up-to-date (i.e., if cache has been invalidated, then this could be wrong).
     */
    protected String getNameFromId(long inodeId) {
        return idToFullPathMap.get(inodeId);
    }

    /**
     * Invalidate the given key. By default, this first checks to see if such an entry already exists in the cache.
     * This check can be skipped by passing `true` for the `skipCheck` argument.
     *
     * This function does not check to see if the key is already in an invalidated state. If this is the case,
     * then this function effectively does nothing.
     *
     * @param key The key to invalidate.
     * @param skipCheck If true, do not bother checking to see if a cache entry exists for the key first.
     * @return True if the key was invalidated, otherwise false. If {@code skipCheck} is true, then this
     * function will always return true.
     */
    protected boolean invalidateKey(String key, boolean skipCheck) {
        return invalidateKeyInternal(key, skipCheck);
    }

    /**
     * This actually invalidates the key.
     * @param key The key to be invalidated.
     * @param skipCheck If true, then we don't bother checking if we are caching the key first
     *                  before we invalidate it. We simply invalidate the key immediately.
     * @return True if we invalidated the key. Otherwise, returns false. If {@code skipCheck} is true, then this
     * function will always return true.
     */
    private boolean invalidateKeyInternal(String key, boolean skipCheck) {
        long s = System.currentTimeMillis();
        _mutex.writeLock().lock();
        try {
            if (skipCheck || containsKeySkipInvalidCheck(key)) {
                prefixMetadataCache.remove(key);
                cache.invalidate(key);
                // fullPathMetadataCache.remove(key);
                return true;
            }

            return false;
        } finally {
            _mutex.writeLock().unlock();
            if (LOG.isTraceEnabled()) LOG.trace("Invalidated key '" + key +
                    "' in " + (System.currentTimeMillis() - s) + " ms.");
        }
    }

    /**
     * Invalidate all entries in the cache.
     */
    protected void invalidateEntireCache() {
        LOG.warn("Invalidating ENTIRE cache. ");
        long s = System.currentTimeMillis();
        _mutex.writeLock().lock();
        try {
            prefixMetadataCache.clear();
        } finally {
            _mutex.writeLock().unlock();
            if (LOG.isTraceEnabled()) LOG.trace("Invalidated entire cache in " + (System.currentTimeMillis() - s) + " ms.");
        }
        idToFullPathMap.clear();
        parentIdPlusLocalNameToFullPathMapping.clear();
        // fullPathMetadataCache.clear();
    }

    /**
     * Invalidate any keys in our cache prefixed by the {@code prefix} parameter.
     *
     * For example, if {@code prefix} were equal to "/home/ben/documents/", then any INodes in our cache that are
     * stored along that path would be invalidated.
     *
     * @param prefix Any metadata prefixed by this path will be invalidated.
     *
     * @return The entries that were prefixed by the specified prefix, so we can invalidate other
     * cached metadata objects (e.g., Ace and EncryptionZone instances).
     */
    protected Collection<INode> invalidateKeysByPrefix(String prefix) {
        long s = System.currentTimeMillis();
        if (LOG.isDebugEnabled()) LOG.debug("Invalidating all INodes prefixed by '" + prefix + "'.");

        _mutex.writeLock().lock();
        List<INode> invalidatedEntries = new ArrayList<>();

        try {
            SortedMap<String, INode> prefixedEntries = prefixMetadataCache.prefixMap(prefix);

            // Avoid concurrent modification exception.
            ArrayList<Map.Entry<String, INode>> toInvalidate = new ArrayList<>(prefixedEntries.entrySet());

            int numInvalidated = 0;
            for (Map.Entry<String, INode> entry : toInvalidate) {
                String path = entry.getKey();
                INode cachedINode = entry.getValue();
                // This if-statement invalidates the key. If we were caching the key, then the call
                // to invalidateKey() returns true, in which case we increment 'numInvalidated'.
                if (invalidateKey(path, false)) {
                    numInvalidated++;
                    invalidatedEntries.add(cachedINode);
                }
            }

            if (LOG.isTraceEnabled()) LOG.trace("Invalidated " + numInvalidated + "/" + prefixedEntries.size() + " of the nodes in the prefix map.");

            return invalidatedEntries;
        } finally {
            _mutex.writeLock().unlock();
            if (LOG.isTraceEnabled()) LOG.trace("Invalidated all cached INodes prefixed by '" + prefix +
                    "' in " + (System.currentTimeMillis() - s) + " ms.");
        }
    }

    public int getNumCacheMissesCurrentRequest() {
        return threadLocalCacheMisses.get();
    }

    public int getNumCacheHitsCurrentRequest() {
        return threadLocalCacheHits.get();
    }
}
