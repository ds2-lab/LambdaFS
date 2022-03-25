package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.transaction.context.BlockPK;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;

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
    private final Cache<Long, Set<Value>> inodeIdsToReplicas;

    /**
     * With this cache, we map Block ID to replicas. Since Block ID by itself is not the primary key of a replica,
     * there could be multiple replica instances associated with a single Block ID. Thus, this is a Cache that maps
     * Block ID to Set<Value>, where Value is some type of replica object.
     */
    private final Cache<Long, Set<Value>> blockIdsToReplicas;

    private ReplicaCacheManager replicaCacheManager;

    protected ReplicaCache() {
        this(DEFAULT_MAX_CACHE_SIZE);
    }

    protected ReplicaCache(int maximumSize) {
        replicaCache = Caffeine.newBuilder().maximumSize(maximumSize).build();
        inodeIdsToReplicas = Caffeine.newBuilder().maximumSize(maximumSize).build();
        blockIdsToReplicas = Caffeine.newBuilder().maximumSize(maximumSize).build();
    }

    /**
     * Invalidate the entry with the given key. The INode ID is also passed in order to invalidate the separate
     * INode ID to multiple-replicas cache.
     * @param key The primary key of the replica to invalidate.
     */
    public void invalidateEntry(Key key) {
        replicaCache.invalidate(key);
        inodeIdsToReplicas.invalidate(key.getInodeId());
        blockIdsToReplicas.invalidate(key.getBlockId());
    }

    /**
     * Invalidate all entires within the cache.
     */
    public void invalidateAll() {
        replicaCache.invalidateAll();
        inodeIdsToReplicas.invalidateAll();
        blockIdsToReplicas.invalidateAll();
    }

    /**
     * Record a cache hit.
     */
    private void cacheHit() {
        if (replicaCacheManager == null) {
            ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
            if (instance == null) return;
            MetadataCacheManager metadataCacheManager = instance.getNamesystem().getMetadataCacheManager();
            replicaCacheManager = metadataCacheManager.getReplicaCacheManager();
        }

        replicaCacheManager.cacheHit();
    }

    /**
     * Record a cache miss.
     */
    private void cacheMiss() {
        if (replicaCacheManager == null) {
            ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
            if (instance == null) return;
            MetadataCacheManager metadataCacheManager = instance.getNamesystem().getMetadataCacheManager();
            replicaCacheManager = metadataCacheManager.getReplicaCacheManager();
        }

        replicaCacheManager.cacheMiss();
    }

    /**
     * Cache a replica.
     * @param key The primary key object of the replica.
     * @param value The replica being cached.
     */
    public void cacheEntry(Key key, Value value) {
        replicaCache.put(key, value);

        long inodeId = key.getInodeId();
        long blockId = key.getBlockId();

        Set<Value> replicasAssociatedWithINodeId = inodeIdsToReplicas.getIfPresent(inodeId);
        if (replicasAssociatedWithINodeId == null) {
            replicasAssociatedWithINodeId = new HashSet<>();
            inodeIdsToReplicas.put(blockId, replicasAssociatedWithINodeId);
        }

        Set<Value> replicasAssociatedWithBlockId = blockIdsToReplicas.getIfPresent(blockId);
        if (replicasAssociatedWithBlockId == null) {
            replicasAssociatedWithBlockId = new HashSet<>();
            blockIdsToReplicas.put(blockId, replicasAssociatedWithBlockId);
        }

        replicasAssociatedWithINodeId.add(value);
    }

    public Value getByPrimaryKey(Key key) {
        Value val = replicaCache.getIfPresent(key);
        if (val == null)
            cacheMiss();
        else
            cacheHit();
        return val;
    }

    public List<Value> getByINodeId(long inodeId) {
        Set<Value> nodes = inodeIdsToReplicas.getIfPresent(inodeId);

        if (nodes != null) {
            cacheHit();
            return new ArrayList<>(nodes);
        }

        cacheMiss();
        return null;
    }

    public List<Value> getByBlockId(long blockId) {
        Set<Value> nodes = blockIdsToReplicas.getIfPresent(blockId);

        if (nodes != null) {
            cacheHit();
            return new ArrayList<>(nodes);
        }

        cacheMiss();
        return null;
    }
}
