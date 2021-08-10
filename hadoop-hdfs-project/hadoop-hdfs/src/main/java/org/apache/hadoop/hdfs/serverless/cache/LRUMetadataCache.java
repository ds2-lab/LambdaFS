package org.apache.hadoop.hdfs.serverless.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Used by Serverless NameNodes to store and retrieve cached metadata.
 */
public class LRUMetadataCache<String, T> extends LinkedHashMap<String, T> {
    private static final int DEFAULT_MAX_ENTRIES = 100;         // Default maximum capacity.
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;     // Default load factor.

    private static final long serialVersionUID = -8140463703331613827L;

    // The maximum capacity.
    private final int maxSize;

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

    /**
     * Override `put()` to disallow null values from being added to the cache.
     */
    @Override
    public T put(String key, T value) {
        if (value == null) {
            throw new IllegalArgumentException("LRUMetadataCache does NOT support null values. Associated key: "
                    + key);
        }

        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, T> eldest) {
        // This method must be overridden if used in a fixed cache.
        // See https://stackoverflow.com/questions/27475797/use-linkedhashmap-to-implement-lru-cache.
        return this.size() > maxSize;
    }
}
