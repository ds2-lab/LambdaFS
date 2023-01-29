package org.apache.hadoop.hdfs.server.namenode.cache;

import io.hops.metadata.hdfs.entity.*;
import io.hops.transaction.context.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Manages the caches specifically related to replicas. I've compartmentalized this functionality into a
 * separate class so as not to clutter the MetadataCacheManager, which manages INodes, Aces, and EncryptionZones.
 */
public class ReplicaCacheManager {
    public static final Logger LOG = LoggerFactory.getLogger(ReplicaCacheManager.class);

    private static ReplicaCacheManager instance;

    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.ReplicaPK, Replica> replicaCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.ReplicaPK, CorruptReplica> corruptReplicaCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> invalidatedBlockCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK, PendingBlockInfo> pendingBlockCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> replicaUnderConstructionCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK, UnderReplicatedBlock> underReplicatedBlockCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> excessReplicaCache;
    private final org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<BlockPK.CachedBlockPK, CachedBlock> cachedBlockCache;

    private final HashMap<Class<? extends BaseReplicaContext<? extends BlockPK, ?>>, org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<? extends BlockPK, ?>> masterCacheMapping;

    private final ThreadLocal<Integer> threadLocalCacheHits = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Integer> threadLocalCacheMisses = ThreadLocal.withInitial(() -> 0);

    private ReplicaCacheManager() {
        LOG.debug("Instantiating the Replica Cache Manager.");

        replicaCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        corruptReplicaCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        invalidatedBlockCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        pendingBlockCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        replicaUnderConstructionCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        underReplicatedBlockCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        excessReplicaCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);
        cachedBlockCache = new org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<>(10_000);

        masterCacheMapping = new HashMap<>();
        masterCacheMapping.put(ReplicaContext.class, replicaCache);
        masterCacheMapping.put(CorruptReplicaContext.class, corruptReplicaCache);
        masterCacheMapping.put(InvalidatedBlockContext.class, invalidatedBlockCache);
        masterCacheMapping.put(PendingBlockContext.class, pendingBlockCache);
        masterCacheMapping.put(ReplicaUnderConstructionContext.class, replicaUnderConstructionCache);
        masterCacheMapping.put(UnderReplicatedBlockContext.class, underReplicatedBlockCache);
        masterCacheMapping.put(ExcessReplicaContext.class, excessReplicaCache);
        masterCacheMapping.put(CachedBlockContext.class, cachedBlockCache);
    }

    public int getThreadLocalCacheHits() { return this.threadLocalCacheHits.get(); }

    public int getThreadLocalCacheMisses() { return this.threadLocalCacheMisses.get(); }

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
     * Set the 'cache hits' and 'cache misses' counters to 0.
     */
    public void resetCacheHitMissCounters() {
        threadLocalCacheHits.set(0);
        threadLocalCacheMisses.set(0);
    }

    public org.apache.hadoop.hdfs.server.namenode.cache.ReplicaCache<? extends BlockPK, ?> getReplicaCache(Class<? extends BaseReplicaContext<?,?>> clazz) {
        if (clazz == null)
            throw new IllegalArgumentException("Class parameter must be non-null.");
        return masterCacheMapping.get(clazz);
    }

    /**
     * Get the singleton ReplicaCacheManager instance. Will instantiate it if it does not already exist.
     */
    protected static ReplicaCacheManager getInstance() {
        if (instance == null)
            instance = new ReplicaCacheManager();

        return instance;
    }
}
