package org.apache.hadoop.hdfs.serverless.cache;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * ** UPDATE**:
 * WE NOW CACHE BY THE FULLY-QUALIFIED PATH OF THE INODE, NOT THE PARENT'S ID.
 *
 * Figure out what the various tables would be called in NDB.
 * The cache (or maybe some other object) will create and maintain ClusterJ Event and EventOperation objects
 * associated with those tables. When event notifications are received, the NameNode will optionally update its
 * cache. May need to add a mechanism for NameNodes to determine if they were the source of the event being fired,
 * as NameNodes should not have to go back to NDB and re-read data that they just wrote.
 *
 * If such a mechanism is not already possible with the current way events are implemented, then events may need
 * to be augmented to somehow relay the identity of the client who last performed the write operation. That may not
 * be feasible, however. In that case, a simpler solution may be to just add a column in the associated tables that
 * denotes the last person to write the data. Then the Event listener can just compare the new value of that column
 * against the local ID, and if they're the same, then the Event listener knows that this NameNode was the source of
 * the Event.
 *
 * In fact, that second solution is probably the best/easiest.
 */

/**
 * Used by Serverless NameNodes to store and retrieve cached metadata.
 */
public class InMemoryINodeCache {
    public static final Logger LOG = LoggerFactory.getLogger(InMemoryINodeCache.class);

    private static final int DEFAULT_MAX_ENTRIES = 30_000;         // Default maximum capacity.
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;         // Default load factor.

    private final ReadWriteLock _mutex = new ReentrantReadWriteLock(true);

    /**
     * This is the main cache, along with the cache HashMap.
     *
     * We use this object when we want to grab a bunch of INodes using a path prefix (e.g., /home/ben/docs/).
     */
    private final PatriciaTrie<INode> prefixMetadataCache;

    /**
     * Cache that is used when not using a prefix.
     */
    private final ConcurrentHashMap<String, INode> fullPathMetadataCache;

    /**
     * Mapping between INode IDs and their names.
     */
    private final ConcurrentHashMap<Long, String> idToNameMapping;

    /**
     * Mapping between keys of the form [PARENT_ID][LOCAL_NAME], which is how some INodes are
     * cached/stored by HopsFS during transactions, to the fully-qualified paths of the INode.
     */
    private final ConcurrentHashMap<String, String> parentIdPlusLocalNameToFullPathMapping;

    private final ThreadLocal<Integer> threadLocalCacheHits = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Integer> threadLocalCacheMisses = ThreadLocal.withInitial(() -> 0);

    private final boolean enabled;

    /**
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public InMemoryINodeCache(Configuration conf) {
        this(conf, DEFAULT_MAX_ENTRIES, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public InMemoryINodeCache(Configuration conf, int capacity, float loadFactor) {
        //this.invalidatedKeys = new HashSet<>();
        this.idToNameMapping = new ConcurrentHashMap<>(capacity, loadFactor);
        this.fullPathMetadataCache = new ConcurrentHashMap<>(capacity, loadFactor);
        this.parentIdPlusLocalNameToFullPathMapping = new ConcurrentHashMap<>(capacity, loadFactor);
        /**
         * This is the main cache, along with the metadataTrie variable. We use this when we want to grab a single
         * INode by its full path.
         */
        this.prefixMetadataCache = new PatriciaTrie<>();
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
    protected void cacheHit() {
        int currentHits = threadLocalCacheHits.get();
        threadLocalCacheHits.set(currentHits + 1);
    }

    /**
     * Record a cache miss.
     */
    protected void cacheMiss() {
        int currentMisses = threadLocalCacheMisses.get();
        threadLocalCacheMisses.set(currentMisses + 1);
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

        //_mutex.readLock().lock();
//        if (LOG.isDebugEnabled()) LOG.debug("Acquired metadata cache read lock in " +
//                (System.currentTimeMillis() - s) + " ms.");
        try {
            long s1 = System.currentTimeMillis();
            INode returnValue = fullPathMetadataCache.get(key);
            long t2 = System.currentTimeMillis();

            if (returnValue == null)
                cacheMiss();
            else
                cacheHit();

            long t3 = System.currentTimeMillis();

            return returnValue;
        } finally {
            //_mutex.readLock().unlock();
            long t4 = System.currentTimeMillis();
            if (LOG.isTraceEnabled() && t4 - s > 10) LOG.trace("Checked cache by path for INode '" + key + "' in " +
                    (t4 - s) + " ms. [1]");
        }
    }

    /**
     * Generate a unique (hopefully) hash code we can use instead of concatenating Strings together.
     */
    private int computeParentIdLocalNameHash(long parentId, String localName) {
        return (int)(parentId ^ localName.hashCode());
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

//        _mutex.readLock().lock();
//        if (LOG.isDebugEnabled()) LOG.debug("Acquired metadata cache read lock in " +
//                (System.currentTimeMillis() - s) + " ms.");
        try {

            String parentIdPlusLocalName = parentId + localName;
            // int parentIdPlusLocalName = computeParentIdLocalNameHash(parentId, localName);
            String key = parentIdPlusLocalNameToFullPathMapping.getOrDefault(parentIdPlusLocalName, null);

            if (key != null)
                return getByPath(key);
            cacheMiss();
            return null;
        } finally {
            //_mutex.readLock().unlock();

            long t = System.currentTimeMillis();
            if (LOG.isTraceEnabled() && t - s > 10) LOG.trace("Checked cache by parent ID and local name for INode '" + localName +
                    "' in " + (t - s) + " ms.");
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
//        _mutex.readLock().lock();
//        if (LOG.isDebugEnabled()) LOG.debug("Acquired metadata cache read lock in " +
//                (System.currentTimeMillis() - s) + " ms.");
        try {
            if (idToNameMapping.containsKey(iNodeId)) {
                String key = idToNameMapping.get(iNodeId);
                return getByPath(key);
            }

            cacheMiss();
            return null;
        } finally {
//            _mutex.readLock().unlock();

            long t = System.currentTimeMillis();
            if (LOG.isTraceEnabled() && t - s > 10) LOG.trace("Checked cache by ID for INode " + iNodeId + " in " +
                    (System.currentTimeMillis() - s) + " ms.");
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

        _mutex.writeLock().lock();
        INode returnValue;
        try {
            // Store the metadata in the cache directly.
            returnValue = prefixMetadataCache.put(key, value);
        } finally {
            _mutex.writeLock().unlock();
            long t = System.currentTimeMillis();
            if (LOG.isTraceEnabled() && t - s > 10) LOG.trace("Stored INode '" + key + "' (ID=" + iNodeId + ") in cache in " +
                    (t - s) + " ms.");
        }

        String parentIdPlusLocalName = value.getParentId() + value.getLocalName();
        // int parentIdPlusLocalName = computeParentIdLocalNameHash(value.getParentId(), value.getLocalName());
        parentIdPlusLocalNameToFullPathMapping.put(parentIdPlusLocalName, key);

        // Create a mapping between the INode ID and the path.
        idToNameMapping.put(iNodeId, key);

        // Cache by full-path.
        fullPathMetadataCache.put(key, value);

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
                String keyAsStr = idToNameMapping.getOrDefault((Long) key, null);
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
            String keyAsStr = idToNameMapping.getOrDefault(inodeId, null);

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
            if (idToNameMapping.containsKey(inodeId)) {
                String key = idToNameMapping.get(inodeId);

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
        return idToNameMapping.get(inodeId);
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
                fullPathMetadataCache.remove(key);
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
        idToNameMapping.clear();
        fullPathMetadataCache.clear();
        parentIdPlusLocalNameToFullPathMapping.clear();
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
                // This if-statement invalidates the key. If we were caching the key, then the call
                // to invalidateKey() returns true, in which case we increment 'numInvalidated'.
                if (invalidateKey(path, false)) {
                    numInvalidated++;
                    invalidatedEntries.add(entry.getValue());
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
