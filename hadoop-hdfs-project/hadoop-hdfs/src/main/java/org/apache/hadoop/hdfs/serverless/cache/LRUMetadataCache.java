package org.apache.hadoop.hdfs.serverless.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Used by Serverless NameNodes to store and retrieve cached metadata.
 */
public class LRUMetadataCache<String, T> extends LinkedHashMap<String, T> {

    private static final int MAX_ENTRIES = 100;
    private static final float LOAD_FACTOR = 0.75f;

    private static final long serialVersionUID = -8140463703331613827L;

    private final int maxSize;

    public LRUMetadataCache() {
        super(MAX_ENTRIES, 0.75f, true);

        this.maxSize = MAX_ENTRIES;
    }

    public LRUMetadataCache(int capacity) {
        super(capacity, 0.75f, true);

        this.maxSize = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, T> eldest) {
        // This method must be overridden if used in a fixed cache.
        // See https://stackoverflow.com/questions/27475797/use-linkedhashmap-to-implement-lru-cache.
        return this.size() > maxSize;
    }
}
