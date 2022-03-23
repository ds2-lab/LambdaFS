package org.apache.hadoop.hdfs.serverless.cache;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.serverless.NuclioHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
public class LRUMetadataCache<T> {
    //static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(LRUMetadataCache.class);

    private static final int DEFAULT_MAX_ENTRIES = 10000;         // Default maximum capacity.
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;       // Default load factor.

    private static final long serialVersionUID = -8140463703331613827L;

    private final Lock _mutex = new ReentrantLock(true);

    // TODO: Add org.apache.commons.collections4.trie.PatriciaTrie to store INodes to support subtree invalidations.
    private final PatriciaTrie<T> metadataTrie;

    /**
     * Keys are added to this set upon being invalidated. If a key is in this set,
     * then the data in the cache for that key is out-of-date and must be retrieved from
     * intermediate storage, rather than from the cache.
     *
     * This must remain a subset of the keyset of the cache variable.
     */
    // private final HashSet<String> invalidatedKeys;

    /**
     * Mapping between INode IDs and their names.
     */
    private final HashMap<Long, String> idToNameMapping;

    /**
     * Mapping between keys of the form [PARENT_ID][LOCAL_NAME], which is how some INodes are
     * cached/stored by HopsFS during transactions, to the fully-qualified paths of the INode.
     */
    private final HashMap<String, String> parentIdPlusLocalNameToFullPathMapping;

    private final HashMap<String, T> cache;

    /**
     * Cache hits experienced across all requests processed by the NameNode.
     */
    private int numCacheHits;

    /**
     * Cache misses experienced across all requests processed by the NameNode.
     */
    private int numCacheMisses;

    /**
     * Cache hits experienced while processing the CURRENT request.
     */
    private int numCacheHitsCurrentRequest;

    /**
     * Cache misses experienced while processing the CURRENT request.
     */
    private int numCacheMissesCurrentRequest;

    private final boolean enabled;

    /**
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public LRUMetadataCache(Configuration conf) {
        this(conf, DEFAULT_MAX_ENTRIES, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public LRUMetadataCache(Configuration conf, int capacity, float loadFactor) {
        //this.invalidatedKeys = new HashSet<>();
        this.idToNameMapping = new HashMap<>(capacity, loadFactor);
        this.parentIdPlusLocalNameToFullPathMapping = new HashMap<>(capacity, loadFactor);
        this.cache = new HashMap<>(capacity, loadFactor);
        this.metadataTrie = new PatriciaTrie<>();
        this.enabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_METADATA_CACHE_ENABLED,
                DFSConfigKeys.SERVERLESS_METADATA_CACHE_ENABLED_DEFAULT);
    }

    /**
     * Update counters that track the number of cache hits there have been.
     */
    private void cacheHit() {
        this.numCacheHits++;
        this.numCacheHitsCurrentRequest++;
    }

    /**
     * Update counters that track the number of cache misses there have been.
     */
    private void cacheMiss() {
        this.numCacheMisses++;
        this.numCacheMissesCurrentRequest++;
    }

    /**
     * Reset the current-request counters for cache hits/misses.
     */
    @Deprecated // TODO: Why deprecated?
    public void clearCurrentRequestCacheCounters() {
        this.numCacheHitsCurrentRequest = 0;
        this.numCacheMissesCurrentRequest = 0;
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param key The fully-qualified path of the desired INode
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public T getByPath(String key) {
        _mutex.lock();
        try {
            if (!enabled)
                return null;

//            if (invalidatedKeys.contains(key)){
//                cacheMiss();
//                return null;
//            }

            T returnValue = cache.get(key);

            if (returnValue == null) {
                cacheMiss();
            }
            else {
                // LOG.debug("Retrieved value " + returnValue + " from cache using key " + key + ".");
                cacheHit();
            }

            return returnValue;
        } finally {
            _mutex.unlock();
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
    public T getByParentINodeIdAndLocalName(long parentId, String localName) {
        _mutex.lock();
        try {
            if (!enabled)
                return null;

            String parentIdPlusLocalName = parentId + localName;
            String key = parentIdPlusLocalNameToFullPathMapping.getOrDefault(parentIdPlusLocalName, null);

//            if (invalidatedKeys.contains(key)){
//                cacheMiss();
//                return null;
//            }

            T returnValue = cache.get(key);

            if (returnValue == null) {
                cacheMiss();
            }
            else {
                // LOG.debug("Retrieved value " + returnValue + " from cache using key " + key + ".");
                cacheHit();
            }

            return returnValue;
        } finally {
            _mutex.unlock();
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
    public T getByINodeId(long iNodeId) {
        _mutex.lock();
        try {
            if (!enabled) {
                cacheMiss();
                return null;
            }

            if (idToNameMapping.containsKey(iNodeId)) {
                String key = idToNameMapping.get(iNodeId);
                return getByPath(key);
            }

            cacheMiss();
            return null;
        } finally {
             _mutex.unlock();
        }
    }

    /**
     * Override `put()` to disallow null values from being added to the cache.
     *
     * @param key The fully-qualified path of the desired INode
     * @param iNodeId The INode ID of the given metadata object.
     * @param value The metadata object to cache under the given key.
     */
    public T put(String key, long iNodeId, T value) {
        if (value == null)
            throw new IllegalArgumentException("LRUMetadataCache does NOT support null values. Associated key: " + key);

        _mutex.lock();
        try {
            // Store the metadata in the cache directly.
            T returnValue = cache.put(key, value);

            if (value instanceof INode) {
                INode valueAsINode = (INode) value;
                String parentIdPlusLocalName = valueAsINode.getParentId() + valueAsINode.getLocalName();
                parentIdPlusLocalNameToFullPathMapping.put(parentIdPlusLocalName, key);
            }

            // Create a mapping between the INode ID and the path.
            idToNameMapping.put(iNodeId, key);

            // Store the metadata in the Trie data structure, using the path of the metadata as the key.
            /*T existingEntry = */
            metadataTrie.put(key, value);

            // If this key was previously invalidated, then it is no longer invalid, seeing as we're caching it now.
            // boolean removed = invalidatedKeys.remove(key);

            //if (removed)
            //    LOG.debug("Previously invalid key '" + key + "' updated with valid cache value. Cache size: " + cache.size());
//            else if (existingEntry == null) // This ensures we only print the message if the object wasn't already cached.
//                LOG.debug("Inserted metadata object into cache under key '" + key + "'. Cache size: " + cache.size());

            return returnValue;
        } finally {
            _mutex.unlock();
        }
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(Object key) {
        if (key == null)
            return false;

        _mutex.lock();
        try {
            // If the given key is a string, then we can use it directly.
            if (key instanceof String) {
                // return !invalidatedKeys.contains(key) && cache.containsKey(key);
                return cache.containsKey(key);
            } else if (key instanceof Long) {
                // If the key is a long, we need to check if we've mapped this long to a String key. If so,
                // then we can get the string version and continue as before.
                String keyAsStr = idToNameMapping.getOrDefault((Long) key, null);
                // return keyAsStr != null && !invalidatedKeys.contains(keyAsStr) && cache.containsKey(keyAsStr);
                return keyAsStr != null && cache.containsKey(keyAsStr);
            }

            return false;
        } finally {
            _mutex.unlock();
        }
    }

    /**
     * Return the size of the cache.
     *
     * @param includeInvalidKeys If true, include invalid keys in the size.
     */
    public int size(boolean includeInvalidKeys) {
        _mutex.lock();
        try {
            int cacheSize = cache.size();

//            if (!includeInvalidKeys)
//                cacheSize -= invalidatedKeys.size();

            return cacheSize;
        } finally {
            _mutex.unlock();
        }
    }

//    /**
//     * Return the number of keys that have been invalidated.
//     */
//    public int numInvalidatedKeys() {
//        return invalidatedKeys.size();
//    }

    /**
     * Check if there is an entry for the given key in this cache, regardless of whether not that entry is
     * marked as invalid or not. That is, this "skips" the check of whether this key is invalid.
     *
     * The regular containsKey() function only returns true if this contains the key AND the key has not been
     * invalidated.
     */
    public boolean containsKeySkipInvalidCheck(String key) {
        _mutex.lock();
        try {
            // Directly check if the cache itself contains the key.
            return cache.containsKey(key);
        } finally {
            _mutex.unlock();
        }
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(long inodeId) {
        // If the key is a long, we need to check if we've mapped this long to a String key. If so,
        // then we can get the string version and continue as before.
        _mutex.lock();
        try {
            // If we don't have a mapping for this key, then this will return null, and this function will return false.
            String keyAsStr = idToNameMapping.getOrDefault(inodeId, null);

            // Returns true if we are able to resolve the NameNode ID to a string-typed key, that key is not
            // invalidated, and we're actively caching the key.
            //return keyAsStr != null && !invalidatedKeys.contains(keyAsStr) && cache.containsKey(keyAsStr);
            return keyAsStr != null && cache.containsKey(keyAsStr);
        } finally {
            _mutex.unlock();
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
        _mutex.lock();
        try  {
            if (idToNameMapping.containsKey(inodeId)) {
                String key = idToNameMapping.get(inodeId);

                return invalidateKeyInternal(key, false);
            }

            return false;
        } finally {
            _mutex.unlock();
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
        _mutex.lock();
        try {
            if (skipCheck || containsKeySkipInvalidCheck(key)) {
                // invalidatedKeys.add(key);

                cache.remove(key);
                metadataTrie.remove(key);

                LOG.debug("Invalidated key " + key + ".");
                return true;
            }

            return false;
        } finally {
            _mutex.unlock();
        }
    }

    /**
     * Invalidate all entries in the cache.
     */
    protected void invalidateEntireCache() {
        LOG.warn("Invalidating ENTIRE cache. ");
        _mutex.lock();
        try {
//            for (String key : cache.keySet())
//                invalidateKey(key, true);
            cache.clear();
            metadataTrie.clear();
        } finally {
            _mutex.unlock();
        }
    }

//    /**
//     * Return the current set of keys in the cache.
//     * @param includeInvalidKeys If true, the invalid keys will also be included in the returned list.
//     */
//    public List<String> getPathKeys(boolean includeInvalidKeys) {
//        ArrayList<String> keyList = new ArrayList<>();
//
//        _mutex.lock();
//        try {
//            for (String key : cache.keySet()) {
//                if (includeInvalidKeys || !invalidatedKeys.contains(key))
//                    keyList.add(key);
//            }
//
//            return keyList;
//        } finally {
//            _mutex.unlock();
//        }
//    }

    /**
     * Get the invalidated keys.
     */
//    public String[] getInvalidatedKeys() {
//        return invalidatedKeys.toArray(new String[0]);
//    }

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
    protected Collection<T> invalidateKeysByPrefix(String prefix) {
        LOG.debug("Invalidating all cached INodes contained within the file subtree rooted at '" + prefix + "'.");

        _mutex.lock();

        try {
            SortedMap<String, T> prefixedEntries = metadataTrie.prefixMap(prefix);
            int numInvalidated = 0;

            for (String path : prefixedEntries.keySet()) {
                // This if-statement invalidates the key. If we were caching the key, then the call
                // to invalidateKey() returns true, in which case we increment 'numInvalidated'.
                if (invalidateKey(path, false))
                    numInvalidated++;
            }

            LOG.debug("Invalidated " + numInvalidated + "/" + prefixedEntries.size() + " of the nodes in the prefix map.");

            return prefixedEntries.values();
        } finally {
            _mutex.unlock();
        }
    }

    public int getNumCacheMissesCurrentRequest() {
        return numCacheMissesCurrentRequest;
    }

    public int getNumCacheHitsCurrentRequest() {
        return numCacheHitsCurrentRequest;
    }

    public int getNumCacheMisses() {
        return numCacheMisses;
    }

    public int getNumCacheHits() {
        return numCacheHits;
    }
}
