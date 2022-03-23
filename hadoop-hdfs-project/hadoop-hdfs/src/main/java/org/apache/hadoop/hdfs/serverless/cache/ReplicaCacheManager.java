package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.metadata.hdfs.entity.*;
import io.hops.transaction.context.*;

import java.util.HashMap;

/**
 * Manages the caches specifically related to replicas. I've compartmentalized this functionality into a
 * separate class so as not to clutter the MetadataCacheManager, which manages INodes, Aces, and EncryptionZones.
 */
public class ReplicaCacheManager {
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

    private ReplicaCacheManager() {
//        replicaCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        corruptReplicaCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        invalidatedBlockCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        pendingBlockCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        replicaUnderConstructionCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        underReplicatedBlockCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        excessReplicaCache = Caffeine.newBuilder().maximumSize(10_000).build();
//        cachedBlockCache = Caffeine.newBuilder().maximumSize(10_000).build();
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

    public ReplicaCache<? extends BlockPK, ?> getCache(BaseReplicaContext<? extends BlockPK, ?> context) {
        if (context == null)
            throw new IllegalArgumentException("Context parameter must be non-null.");
        return masterCacheMapping.get(context.getClass());
    }

    public ReplicaCache<? extends BlockPK, ?> getCache(Class<BaseReplicaContext<? extends BlockPK, ?>> clazz) {
        if (clazz == null)
            throw new IllegalArgumentException("Class parameter must be non-null.");
        return masterCacheMapping.get(clazz);
    }

    protected ReplicaCacheManager getInstance() {
        if (instance == null)
            instance = new ReplicaCacheManager();

        return instance;
    }
}
