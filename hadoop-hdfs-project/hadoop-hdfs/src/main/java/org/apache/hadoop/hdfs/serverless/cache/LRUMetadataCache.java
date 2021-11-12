package org.apache.hadoop.hdfs.serverless.cache;

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
    static final Logger LOG = LoggerFactory.getLogger(LRUMetadataCache.class);

    private static final int DEFAULT_MAX_ENTRIES = 250;         // Default maximum capacity.
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;     // Default load factor.

    private static final long serialVersionUID = -8140463703331613827L;

    private final Lock _mutex = new ReentrantLock(true);

    /**
     * Keys are added to this set upon being invalidated. If a key is in this set,
     * then the data in the cache for that key is out-of-date and must be retrieved from
     * intermediate storage, rather than from the cache.
     *
     * This must remain a subset of the keyset of the cache variable.
     */
    private final HashSet<String> invalidatedKeys;

    /**
     * Mapping between INode IDs and their names.
     */
    private final HashMap<Long, String> idToNameMapping;

    private final HashMap<String, T> cache;

    /**
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public LRUMetadataCache() {
        this(DEFAULT_MAX_ENTRIES, DEFAULT_LOAD_FACTOR);
    }

//    /**
//     * Create an LRU Metadata Cache using the default load factor value and a specified maximum capacity.
//     */
//    public LRUMetadataCache(int capacity) {
//        this(capacity, DEFAULT_LOAD_FACTOR);
//    }
//
//
//    /**
//     * Create an LRU Metadata Cache using the default maximum capacity and a specified load factor value.
//     */
//    public LRUMetadataCache(float loadFactor) {
//        this(DEFAULT_MAX_ENTRIES, loadFactor);
//    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public LRUMetadataCache(int capacity, float loadFactor) {
        this.invalidatedKeys = new HashSet<>();
        this.idToNameMapping = new HashMap<>(capacity, loadFactor);
        this.cache = new HashMap<>(capacity, loadFactor);
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param key The fully-qualified path of the desired INode
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public synchronized T getByPath(String key) {
        if (invalidatedKeys.contains(key))
            return null;

        T returnValue = cache.get(key);

        LOG.debug("Retrieved value " + returnValue.toString() + " from cache using key " + key + ".");

        return returnValue;
    }

    /**
     * Return the metadata object associated with the given key, or null if:
     *  (1) No entry exists for the given key, or
     *  (2) The given key has been invalidated.
     * @param iNodeId The INode ID of the given metadata object.
     * @return The metadata object cached under the given key, or null if no such mapping exists or the key has been
     * invalidated.
     */
    public synchronized T getByINodeId(long iNodeId) {
        if (idToNameMapping.containsKey(iNodeId)) {
            String key = idToNameMapping.get(iNodeId);
            return getByPath(key);
        }
        return null;
    }

    /**
     * Override `put()` to disallow null values from being added to the cache.
     *
     * @param key The fully-qualified path of the desired INode
     * @param iNodeId The INode ID of the given metadata object.
     * @param value The metadata object to cache under the given key.
     */
    public synchronized T put(String key, long iNodeId, T value) {
        if (value == null)
            throw new IllegalArgumentException("LRUMetadataCache does NOT support null values. Associated key: " + key);

        T returnValue = cache.put(key, value);
        idToNameMapping.put(iNodeId, key);

        boolean removed = invalidatedKeys.remove(key);

        if (removed)
            LOG.debug("Previously invalid key " + key + " updated with valid cache value. Cache size: " + cache.size());
        else
            LOG.debug("Inserted metadata object into cache under key " + key + ". Cache size: " + cache.size());

        return returnValue;
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(Object key) {
        if (key == null)
            return false;

        // If the given key is a string, then we can use it directly.
        if (key instanceof String) {
            return !invalidatedKeys.contains(key) && cache.containsKey(key);
        }
        else if (key instanceof Long) {
            // If the key is a long, we need to check if we've mapped this long to a String key. If so,
            // then we can get the string version and continue as before.
            String keyAsStr = idToNameMapping.getOrDefault((Long)key, null);
            return keyAsStr != null && !invalidatedKeys.contains(keyAsStr) && cache.containsKey(keyAsStr);
        }

        return false;
    }

    /**
     * Return the size of the cache.
     *
     * @param includeInvalidKeys If true, include invalid keys in the size.
     */
    public int size(boolean includeInvalidKeys) {
        int cacheSize = cache.size();

        if (!includeInvalidKeys)
            cacheSize -= invalidatedKeys.size();

        return cacheSize;
    }

    /**
     * Return the number of keys that have been invalidated.
     */
    public int numInvalidatedKeys() {
        return invalidatedKeys.size();
    }

    /**
     * Check if there is an entry for the given key in this cache, regardless of whether not that entry is
     * marked as invalid or not. That is, this "skips" the check of whether this key is invalid.
     *
     * The regular containsKey() function only returns true if this contains the key AND the key has not been
     * invalidated.
     */
    public boolean containsKeySkipInvalidCheck(String key) {
        return cache.containsKey(key);
    }

    /**
     * Check if there is an entry for the given key in this cache, regardless of whether not that entry is
     * marked as invalid or not. That is, this "skips" the check of whether this key is invalid.
     */
    public boolean containsKeySkipInvalidCheck(long inodeId) {
        // If the key is a long, we need to check if we've mapped this long to a String key. If so,
        // then we can get the string version and continue as before.
        String keyAsStr = idToNameMapping.getOrDefault(inodeId, null);
        return keyAsStr != null && cache.containsKey(keyAsStr);
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    public boolean containsKey(long inodeId) {
        // If the key is a long, we need to check if we've mapped this long to a String key. If so,
        // then we can get the string version and continue as before.
        String keyAsStr = idToNameMapping.getOrDefault(inodeId, null);
        return keyAsStr != null && !invalidatedKeys.contains(keyAsStr) && cache.containsKey(keyAsStr);
    }

    /**
     * Invalidate a given key by passing the INode ID of the INode to be invalidated, rather than the
     * fully-qualified path.
     * @param inodeId The INode ID of the INode to be invalidated.
     *
     * @return True if the key was invalidated, otherwise false.
     */
    public boolean invalidateKey(long inodeId) {
        if (idToNameMapping.containsKey(inodeId)) {
            String key = idToNameMapping.get(inodeId);

            return invalidateKey(key, false);
        }

        return false;
    }

    /**
     * Invalidate all entries in the cache.
     */
    public void invalidateEntireCache() {
        LOG.warn("Invalidating ENTIRE cache. ");

        for (String key : cache.keySet())
            invalidateKey(key, true);
    }

    /**
     * Return the current set of keys in the cache.
     * @param includeInvalidKeys If true, the invalid keys will also be included in the returned list.
     */
    public List<String> getPathKeys(boolean includeInvalidKeys) {
        ArrayList<String> keyList = new ArrayList<>();

        for (String key : cache.keySet()) {
            if (includeInvalidKeys || !invalidatedKeys.contains(key))
                keyList.add(key);
        }

        return keyList;
    }

    /**
     * Get the invalidated keys.
     */
    public String[] getInvalidatedKeys() {
        return invalidatedKeys.toArray(new String[0]);
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
     * @return True if the key was invalidated, otherwise false.
     */
    public boolean invalidateKey(String key, boolean skipCheck) {
        if (skipCheck || containsKeySkipInvalidCheck(key)) {
            invalidatedKeys.add(key);
            LOG.debug("Invalidated key " + key + ".");
            return true;
        }

        return false;
    }
}
