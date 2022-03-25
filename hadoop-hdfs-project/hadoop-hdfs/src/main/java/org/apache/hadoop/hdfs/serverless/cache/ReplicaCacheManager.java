package org.apache.hadoop.hdfs.serverless.cache;

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

    private final ReplicaCache<BlockPK.ReplicaPK, Replica> replicaCache;
    private final ReplicaCache<BlockPK.ReplicaPK, CorruptReplica> corruptReplicaCache;
    private final ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> invalidatedBlockCache;
    private final ReplicaCache<BlockPK, PendingBlockInfo> pendingBlockCache;
    private final ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> replicaUnderConstructionCache;
    private final ReplicaCache<BlockPK, UnderReplicatedBlock> underReplicatedBlockCache;
    private final ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> excessReplicaCache;
    private final ReplicaCache<BlockPK.CachedBlockPK, CachedBlock> cachedBlockCache;

    private final HashMap<Class<? extends BaseReplicaContext<? extends BlockPK, ?>>, ReplicaCache<? extends BlockPK, ?>> masterCacheMapping;

    private final ThreadLocal<Integer> threadLocalCacheHits = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Integer> threadLocalCacheMisses = ThreadLocal.withInitial(() -> 0);

    private ReplicaCacheManager() {
        LOG.debug("Instantiating the Replica Cache Manager.");

        replicaCache = new ReplicaCache<>(10_000);
        corruptReplicaCache = new ReplicaCache<>(10_000);
        invalidatedBlockCache = new ReplicaCache<>(10_000);
        pendingBlockCache = new ReplicaCache<>(10_000);
        replicaUnderConstructionCache = new ReplicaCache<>(10_000);
        underReplicatedBlockCache = new ReplicaCache<>(10_000);
        excessReplicaCache = new ReplicaCache<>(10_000);
        cachedBlockCache = new ReplicaCache<>(10_000);

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

    public ReplicaCache<? extends BlockPK, ?> getReplicaCache(Class<? extends BaseReplicaContext<?,?>> clazz) {
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
