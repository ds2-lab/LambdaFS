package org.apache.hadoop.hdfs.serverless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.metadata.hdfs.entity.EncryptionZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_METADATA_CACHE_CAPACITY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_METADATA_CACHE_CAPACITY_DEFAULT;

/**
 * Controls and manages access to several caches, each of which is responsible for caching a different type of metadata.
 *
 * The main cache is the {@link InMemoryINodeCache}. This is the cache that stores the INodes, which are the primary
 * metadata object used by HopsFS. This class also manages a cache of {@link Ace} objects and a cache of
 * {@link EncryptionZone} objects.
 *
 * The other caches are of type {@link ReplicaCache}, and these are managed by a separate {@link ReplicaCacheManager}.
 */
public class MetadataCacheManager {
    public static final Logger LOG = LoggerFactory.getLogger(MetadataCacheManager.class);

    /**
     * Caches INodes.
     */
    private final InMemoryINodeCache inodeCache;

    /**
     * Caches EncryptionZone instances. The key is INode ID.
     */
    private final Cache<Long, EncryptionZone> encryptionZoneCache;

    /**
     * Cache of Ace instances. Key is a string of the form [INodeID]-[Index], which is
     * the primary key of Ace instances in intermediate storage (NDB specifically).
     */
    private final Cache<String, Ace> aceCache;

    /**
     * We also maintain a list of all ace instances associated with a given INode,
     * so that we can invalidate these entries if the given INode gets modified.
     */
    private final Cache<Long, Set<CachedAce>> aceCacheByINodeId;

    /**
     * Manages the caches associated with the various types of replicas.
     */
    private final ReplicaCacheManager replicaCacheManager;

    /**
     * Maximum elements in INode cache.
     */
    private final int cacheCapacity;

    public MetadataCacheManager(Configuration configuration, int deploymentNumber) {
        this.cacheCapacity = configuration.getInt(SERVERLESS_METADATA_CACHE_CAPACITY, SERVERLESS_METADATA_CACHE_CAPACITY_DEFAULT);
        inodeCache = new InMemoryINodeCache(configuration, deploymentNumber);
        encryptionZoneCache = Caffeine.newBuilder()
                .maximumSize(cacheCapacity)
                .build();
        aceCache = Caffeine.newBuilder()
                .maximumSize(cacheCapacity)
                .build();
        aceCacheByINodeId = Caffeine.newBuilder()
                .maximumSize(cacheCapacity)
                .build();

//        encryptionZoneCache = new ConcurrentHashMap<>();
//        aceCache = new ConcurrentHashMap<>();
//        aceCacheByINodeId = new ConcurrentHashMap<>();

        this.replicaCacheManager = ReplicaCacheManager.getInstance();
    }

    public ReplicaCacheManager getReplicaCacheManager() { return this.replicaCacheManager; }

    public InMemoryINodeCache getINodeCache() { return inodeCache; }

    public int invalidateINodesByPrefix(String prefix) {
        Collection<INode> prefixedINodes = inodeCache.invalidateKeysByPrefix(prefix);

        if (prefixedINodes == null) return 0;

        for (INode node : prefixedINodes) {
            long inodeId = node.getId();
            invalidateAces(inodeId);
            encryptionZoneCache.invalidate(inodeId);
        }

        return prefixedINodes.size();
    }

    public boolean invalidateINode(String key, boolean skipCheck) {
        INode node = inodeCache.getByPath(key);

        if (node != null) {
            long inodeId = node.getId();
            invalidateAces(inodeId);
            encryptionZoneCache.invalidate(inodeId);
            //encryptionZoneCache.remove(inodeId);
        }

        return inodeCache.invalidateKey(key, skipCheck);
    }

    public void invalidateAllINodes() {
        encryptionZoneCache.invalidateAll();
        aceCache.invalidateAll();
        aceCacheByINodeId.invalidateAll();
//        encryptionZoneCache.clear();
//        aceCache.clear();
//        aceCacheByINodeId.clear();
        inodeCache.invalidateEntireCache();
    }

    public boolean invalidateINode(long inodeId) {
        invalidateAces(inodeId);
        encryptionZoneCache.invalidate(inodeId);
        //encryptionZoneCache.remove(inodeId);
        return inodeCache.invalidateKey(inodeId);
    }

    private void invalidateAces(long inodeId) {
        Set<CachedAce> cachedAces = aceCacheByINodeId.getIfPresent(inodeId); // aceCacheByINodeId.getOrDefault(inodeId, null);

        if (cachedAces == null)
            return;

        for (CachedAce cachedAce : cachedAces) {
            String key = getAceKey(cachedAce.inodeId, cachedAce.index);
            aceCache.invalidate(key);
            //aceCache.remove(key);
        }
    }

    /**
     * Return the EncryptionZone cached at the given key, or null if it does not exist.
     * @param inodeId The ID of the associated INode.
     * @return The EncryptionZone cached at the given key, or null if it does not exist.
     */
    public EncryptionZone getEncryptionZone(long inodeId) {
        return encryptionZoneCache.getIfPresent(inodeId);
        //return encryptionZoneCache.getOrDefault(inodeId, null);
    }

    /**
     * Cache the given EncryptionZone instance at the given key.
     */
    public void putEncryptionZone(long inodeId, EncryptionZone encryptionZone) {
        encryptionZoneCache.put(inodeId, encryptionZone);
    }

    /**
     * Return the Ace instance cached with the given INode ID and index field.
     * Returns null if no such Ace instance exists.
     */
    public Ace getAce(long inodeId, int index) {
        String key = getAceKey(inodeId, index);
        return aceCache.getIfPresent(key);
        //return aceCache.getOrDefault(key,null);
    }

    /**
     * Cache the given Ace object with a key generated by the INode ID and the index.
     */
    public void putAce(long inodeId, int index, Ace ace) {
        String key = getAceKey(inodeId, index);
        aceCache.put(key, ace);

        CachedAce cachedAce = new CachedAce(inodeId, index, ace);
        Set<CachedAce> cachedAces = aceCacheByINodeId.getIfPresent(inodeId); // aceCacheByINodeId.getOrDefault(inodeId, null);

        if (cachedAces == null) {
            cachedAces = new HashSet<>();
            aceCacheByINodeId.put(inodeId, cachedAces);
        }

        cachedAces.add(cachedAce);
    }

    /**
     * Return the key generated by a given INode ID and an index (for an Ace instance).
     */
    private String getAceKey(long inodeId, int index) {
        return inodeId + "-" + index;
    }

    /**
     * We maintain two Caches for Ace instances. One cache maps their primary key (INode ID and index) to a singular
     * Ace index. The other cache maps INode IDs to CachedAce instances. We do this so that, if the INode gets
     * invalidated, then we can find all the Ace instances we have cached for that INode and invalidate them
     * as well.
     */
    private static class CachedAce {
        /**
         * INode ID of the INode associated with this Ace object.
         */
        long inodeId;

        /**
         * Index/ID of this Ace object. Used as part of the primary key.
         */
        int index;

        /**
         * The actual Ace object that we're caching (and that this class is wrapping).
         */
        Ace ace;

        CachedAce(long inodeId, int index, Ace ace) {
            this.inodeId = inodeId;
            this.index = index;
            this.ace = ace;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedAce){
                CachedAce other = (CachedAce) obj;
                return inodeId == other.inodeId && index == other.index;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + index;
            hash = 31 * hash + Long.hashCode(inodeId);
            return hash;
        }
    }
}
