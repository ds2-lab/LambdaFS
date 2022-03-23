package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.transaction.context.BlockPK;

/**
 * Basic cache used for caching replicas.
 * @param <Key>
 * @param <Value>
 */
public class ReplicaCache<Key extends BlockPK, Value> {
    public static final int DEFAULT_MAX_CACHE_SIZE = 10_000;

    private final Cache<Key, Value> replicaCache;

    protected ReplicaCache() {
        this(DEFAULT_MAX_CACHE_SIZE);
    }

    protected ReplicaCache(int maximumSize) {
        replicaCache = Caffeine.newBuilder().maximumSize(maximumSize).build();
    }

    public void invalidateEntry(Key key) {
        replicaCache.invalidate(key);
    }

    public void invalidateAll() {
        replicaCache.invalidateAll();
    }

    public void cacheEntry(Key key, Value value) {
        replicaCache.put(key, value);
    }
}
