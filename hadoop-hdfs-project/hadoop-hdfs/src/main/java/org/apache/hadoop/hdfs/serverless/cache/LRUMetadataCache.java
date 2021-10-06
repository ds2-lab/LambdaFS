package org.apache.hadoop.hdfs.serverless.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Create an LRU Metadata Cache using the default maximum capacity and load factor values.
     */
    public LRUMetadataCache() {
        super(DEFAULT_MAX_ENTRIES, 0.75f, true);

        this.maxSize = DEFAULT_MAX_ENTRIES;
    }

    /**
     * Create an LRU Metadata Cache using the default load factor value and a specified maximum capacity.
     */
    public LRUMetadataCache(int capacity) {
        super(capacity, DEFAULT_LOAD_FACTOR, true);

        this.maxSize = capacity;
    }


    /**
     * Create an LRU Metadata Cache using the default maximum capacity and a specified load factor value.
     */
    public LRUMetadataCache(float loadFactor) {
        super(DEFAULT_MAX_ENTRIES, loadFactor, true);

        this.maxSize = DEFAULT_MAX_ENTRIES;
    }

    /**
     * Create an LRU Metadata Cache using a specified maximum capacity and load factor.
     */
    public LRUMetadataCache(int capacity, float loadFactor) {
        super(capacity, loadFactor, true);

        this.maxSize = capacity;
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

        LOG.debug("Inserted metadata object into cache under key " + key + ". Cache size: " + this.size());

        //_mutex.unlock();

        return returnValue;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, T> eldest) {
        // This method must be overridden if used in a fixed cache.
        // See https://stackoverflow.com/questions/27475797/use-linkedhashmap-to-implement-lru-cache.
        return this.size() > maxSize;
    }
}
