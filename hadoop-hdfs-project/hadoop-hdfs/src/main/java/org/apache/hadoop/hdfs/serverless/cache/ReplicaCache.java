package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.transaction.context.BlockPK;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Basic cache used for caching replicas.
 * @param <Key>
 * @param <Value>
 */
public class ReplicaCache<Key extends BlockPK, Value> {
    public static final int DEFAULT_MAX_CACHE_SIZE = 10_000;

    private final Cache<Key, Value> replicaCache;

    /**
     * With this cache, we map INode ID to replicas. Since INode ID by itself is not the primary key of a replica,
     * there could be multiple replica instances associated with a single INode ID. Thus, this is a Cache that maps
     * INode ID to Set<Value>, where Value is some type of replica object.
     */
    private final Cache<Long, Set<Value>> idReplicaCache;

    protected ReplicaCache() {
        this(DEFAULT_MAX_CACHE_SIZE);
    }

    protected ReplicaCache(int maximumSize) {
        replicaCache = Caffeine.newBuilder().maximumSize(maximumSize).build();
        idReplicaCache = Caffeine.newBuilder().maximumSize(maximumSize).build();
    }

    /**
     * Invalidate the entry with the given key.
     * @param key The primary key of the replica to invalidate.
     */
    public void invalidateEntry(Key key) {
        replicaCache.invalidate(key);
    }

    /**
     * Invalidate the entry with the given key. The INode ID is also passed in order to invalidate the separate
     * INode ID to multiple-replicas cache.
     * @param key The primary key of the replica to invalidate.
     * @param id The INode ID of the replica we're invalidating, so we can invalidate other replicas.
     */
    public void invalidateEntry(Key key, long id) {
        replicaCache.invalidate(key);
        idReplicaCache.invalidate(id);
    }

    /**
     * Invalidate all entires within the cache.
     */
    public void invalidateAll() {
        replicaCache.invalidateAll();
        idReplicaCache.invalidateAll();
    }

    /**
     * Cache a replica.
     * @param key The primary key object of the replica.
     * @param value The replica being cached.
     * @param id The INode ID associated with the replica. This will be a component of the primary key.
     */
    public void cacheEntry(Key key, Value value, long id) {
        replicaCache.put(key, value);

        Set<Value> replicasAssociatedWithINodeId = idReplicaCache.getIfPresent(id);
        if (replicasAssociatedWithINodeId == null) {
            replicasAssociatedWithINodeId = new HashSet<>();
            idReplicaCache.put(id, replicasAssociatedWithINodeId);
        }

        replicasAssociatedWithINodeId.add(value);
    }
}
