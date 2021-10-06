package org.apache.hadoop.hdfs.serverless.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO:
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
public class LRUMetadataCache<String, T> extends LinkedHashMap<String, T> {
    static final Logger LOG = LoggerFactory.getLogger(LRUMetadataCache.class);

    private static final int DEFAULT_MAX_ENTRIES = 100;         // Default maximum capacity.
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;     // Default load factor.

    private static final long serialVersionUID = -8140463703331613827L;

    // The maximum capacity.
    private final int maxSize;

    private final Lock _mutex = new ReentrantLock(true);

    /**
     * Keys are added to this set upon being invalidated. If a key is in this set,
     * then the data in the cache for that key is out-of-date and must be retrieved from
     * intermediate storage, rather than from the cache.
     */
    private final HashSet<String> invalidatedKeys;

    /**
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public LRUMetadataCache() {
        this(DEFAULT_MAX_ENTRIES, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Create an LRU Metadata Cache using the default load factor value and a specified maximum capacity.
     */
    public LRUMetadataCache(int capacity) {
        this(capacity, DEFAULT_LOAD_FACTOR);
    }


    /**
     * Create an LRU Metadata Cache using the default maximum capacity and a specified load factor value.
     */
    public LRUMetadataCache(float loadFactor) {
        this(DEFAULT_MAX_ENTRIES, loadFactor);
    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public LRUMetadataCache(int capacity, float loadFactor) {
        super(capacity, loadFactor, true);

        this.maxSize = capacity;
        this.invalidatedKeys = new HashSet<>();
    }

    @Override
    public synchronized T get(Object key) {
        //_mutex.lock();

        T returnValue = super.get(key);

        LOG.debug("Retrieved value " + returnValue.toString() + " from cache using key " + key + ".");

        //_mutex.unlock();

        return returnValue;
    }

    /**
     * Override `put()` to disallow null values from being added to the cache.
     */
    @Override
    public synchronized T put(String key, T value) {
        if (value == null)
            throw new IllegalArgumentException("LRUMetadataCache does NOT support null values. Associated key: " + key);

        //_mutex.lock();

        T returnValue = super.put(key, value);

        boolean removed = invalidatedKeys.remove(key);

        if (removed)
            LOG.debug("Previously invalid key " + key + " updated with valid cache value. Cache size: " + size());
        else
            LOG.debug("Inserted metadata object into cache under key " + key + ". Cache size: " + this.size());

        //_mutex.unlock();

        return returnValue;
    }

    /**
     * Checks that the key has not been invalidated before checking the contents of the cache.
     */
    @Override
    public boolean containsKey(Object key) {
        if (key == null)
            return false;

        if (invalidatedKeys.contains((String)key)) {
            return false;
        }

        return super.containsKey(key);
    }

    /**
     * Check if there is an entry for the given key in this cache, regardless of whether not that entry is
     * marked as invalid or not.
     */
    public boolean containsKeySkipInvalidCheck(Object key) {
        return super.containsKey(key);
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
            LOG.debug("Invalidated key " + key + ".");
            invalidatedKeys.add(key);
            return true;
        }

        return false;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, T> eldest) {
        // This method must be overridden if used in a fixed cache.
        // See https://stackoverflow.com/questions/27475797/use-linkedhashmap-to-implement-lru-cache.
        return this.size() > maxSize;
    }
}
