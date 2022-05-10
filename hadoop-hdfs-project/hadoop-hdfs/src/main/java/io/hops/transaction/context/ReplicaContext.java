/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, Replica> {
  public static final Logger LOG = LoggerFactory.getLogger(ReplicaContext.class);

  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  private ReplicaCache<BlockPK.ReplicaPK, Replica> getReplicaCache() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null)
      return null;

    return (ReplicaCache<BlockPK.ReplicaPK, Replica>) instance.getNamesystem().getMetadataCacheManager().getReplicaCacheManager().getReplicaCache(this.getClass());
  }

  private Replica checkCache(long inodeId, long blockId, int storageId) {
    if (!EntityContext.areMetadataCacheReadsEnabled()) return null;

    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return null;

    BlockPK.ReplicaPK pk = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    return cache.getByPrimaryKey(pk);
  }

  // Uses same semantics as the `findByPK()` function.
  private Replica checkCacheByPk(long blockId, int storageId) {
    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return null;

    List<Replica> possibleReplicas = cache.getByBlockId(blockId);

    if (possibleReplicas == null) return null;

    for (Replica replica : possibleReplicas) {
      if (replica.getStorageId() == storageId)
        return replica;
    }

    return null;
  }

  private List<Replica> checkCacheByINodeId(long inodeId) {
    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByINodeId(inodeId);
  }

  private List<Replica> checkCacheByBlockId(long blockId) {
    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByBlockId(blockId);
  }

  private void updateCache(Replica replica) {
    if (replica == null) return;

    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return;

    cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
  }

  private void updateCache(List<Replica> replicas) {
    if (replicas == null) return;

    ReplicaCache<BlockPK.ReplicaPK, Replica> cache = getReplicaCache();
    if (cache == null) return;

    for (Replica replica : replicas) {
      cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
    }
  }

  @Override
  public void update(Replica replica)
      throws TransactionContextException {
    super.update(replica);
    if(isLogTraceEnabled()) {
      log("updated-replica", "bid", replica.getBlockId(), "sid",
              replica.getStorageId());
    }
  }

  @Override
  public void remove(Replica replica)
      throws TransactionContextException {
    super.remove(replica);
    if(isLogTraceEnabled()) {
      log("removed-replica", "bid", replica.getBlockId(), "sid",
              replica.getStorageId());
    }
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<Replica> findList(FinderType<Replica> finder,
      Object... params) throws TransactionContextException, StorageException {
    Replica.Finder iFinder = (Replica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case ByINodeIds:
        return findyByINodeIds(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Replica find(FinderType<Replica> finder,
      Object... params) throws TransactionContextException, StorageException {
    Replica.Finder iFinder = (Replica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndStorageId:
        return findByPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(Replica hopReplica) {
    return new BlockPK.ReplicaPK(hopReplica.getBlockId(),
        hopReplica.getInodeId(), hopReplica.getStorageId());
  }

  @Override
  Replica cloneEntity(Replica hopReplica) {
    return cloneEntity(hopReplica, hopReplica.getInodeId());
  }

  @Override
  Replica cloneEntity(Replica hopReplica, long inodeId) {
    return new Replica(hopReplica.getStorageId(), hopReplica.getBlockId(),
         inodeId, hopReplica.getBucketId());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty();
  }

  private Replica findByPK(Replica.Finder iFinder,
      Object[] params) {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    Replica result = checkCacheByPk(blockId, storageId);
    if (result != null) return result;

    List<Replica> replicas = getByBlock(blockId);
    if (replicas != null) {
      for (Replica replica : replicas) {
        if (replica != null) {
          if (replica.getStorageId() == storageId) {
            result = replica;
            break;
          }
        }
      }
    }
    hit(iFinder, result, "bid", blockId, "sid", storageId);
    return result;
  }

  private List<Replica> findByBlockId(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];

    List<Replica> results = checkCacheByBlockId(blockId);
    if (results != null) return results;
    results = checkCacheByINodeId(inodeId);
    if (results != null && storageCallPrevented) return results;

    if (containsByBlock(blockId) || (containsByINode(inodeId) && storageCallPrevented)) {
      results = getByBlock(blockId);
      hit(iFinder, results, "bid", blockId);
    } else {
      if (LOG.isDebugEnabled()) LOG.debug("Going to NDB to find Replicas for INode ID=" + inodeId + ", BlockID=" + blockId);
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasById(blockId, inodeId);
      updateCache(results);
      gotFromDB(new BlockPK(blockId, null), results);
      miss(iFinder, results, "bid", blockId, "inodeid", inodeId);
    }
    return results;
  }

  private List<Replica> findByINodeId(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];

    List<Replica> results = checkCacheByINodeId(inodeId);
    if (results != null) return results;

    if (containsByINode(inodeId)) {
      results = getByINode(inodeId);
      hit(iFinder, results, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasByINodeId(inodeId);
      updateCache(results);
      gotFromDB(new BlockPK(null, inodeId), results);
      miss(iFinder, results, "inodeid", inodeId);
    }
    return results;
  }

  private List<Replica> findyByINodeIds(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    long[] ids = (long[]) params[0];
    aboutToAccessStorage(iFinder, params);
    List<Replica> results = dataAccess.findReplicasByINodeIds(ids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(ids), results);
    miss(iFinder, results, "inodeIds", Arrays.toString(ids));
    return results;
  }
}
