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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCache;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicaUnderConstructionContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, ReplicaUnderConstruction> {
  public static final Logger LOG = LoggerFactory.getLogger(ReplicaUnderConstructionContext.class);

  ReplicaUnderConstructionDataAccess dataAccess;

  public ReplicaUnderConstructionContext(
      ReplicaUnderConstructionDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  private ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> getReplicaCache() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null)
      return null;

    return (ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction>) instance.getNamesystem().getMetadataCacheManager().getReplicaCacheManager().getReplicaCache(this.getClass());
  }

  private ReplicaUnderConstruction checkCache(long inodeId, long blockId, int storageId) {
    if (!EntityContext.isLocalMetadataCacheEnabled()) return null;

    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return null;

    BlockPK.ReplicaPK pk = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    return cache.getByPrimaryKey(pk);
  }

  // Uses same semantics as the `findByPK()` function.
  private ReplicaUnderConstruction checkCacheByPk(long blockId, int storageId) {
    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return null;

    List<ReplicaUnderConstruction> possibleReplicas = cache.getByBlockId(blockId);

    if (possibleReplicas == null) return null;

    for (ReplicaUnderConstruction replica : possibleReplicas) {
      if (replica.getStorageId() == storageId)
        return replica;
    }

    return null;
  }

  private List<ReplicaUnderConstruction> checkCacheByINodeId(long inodeId) {
    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByINodeId(inodeId);
  }

  private List<ReplicaUnderConstruction> checkCacheByBlockId(long blockId) {
    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByBlockId(blockId);
  }

  private void updateCache(ReplicaUnderConstruction replica) {
    if (replica == null) return;

    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return;

    cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
  }

  private void updateCache(List<ReplicaUnderConstruction> replicas) {
    if (replicas == null) return;

    ReplicaCache<BlockPK.ReplicaPK, ReplicaUnderConstruction> cache = getReplicaCache();
    if (cache == null) return;

    for (ReplicaUnderConstruction replica : replicas) {
      cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
    }
  }

  @Override
  public void update(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.update(replica);
    if(isLogTraceEnabled()) {
      log("added-replicauc", "bid", replica.getBlockId(), "sid",
              replica.getStorageId(), "state", replica.getState().name());
    }
  }

  @Override
  public void remove(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.remove(replica);
    if(isLogTraceEnabled()) {
      log("removed-replicauc", "bid", replica.getBlockId(), "sid",
              replica.getStorageId(), "state", replica.getState().name());
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<ReplicaUnderConstruction> findList(
      FinderType<ReplicaUnderConstruction> finder, Object... params)
      throws TransactionContextException, StorageException {
    ReplicaUnderConstruction.Finder rFinder =
        (ReplicaUnderConstruction.Finder) finder;
    switch (rFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(rFinder, params);
      case ByINodeId:
        return findByINodeId(rFinder, params);
      case ByINodeIds:
        return findByINodeIds(rFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(ReplicaUnderConstruction replica) {
    return new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(),
        replica.getStorageId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction) {
    return cloneEntity(replicaUnderConstruction,
        replicaUnderConstruction.getInodeId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction, long inodeId) {
    return new ReplicaUnderConstruction(
        replicaUnderConstruction.getState(),
        replicaUnderConstruction.getStorageId(),
        replicaUnderConstruction.getBlockId(), inodeId,
        replicaUnderConstruction.getBucketId(),
        replicaUnderConstruction.getGenerationStamp());
  }

  private List<ReplicaUnderConstruction> findByBlockId(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];

    List<ReplicaUnderConstruction> result = checkCacheByBlockId(blockId);
    if (result != null) return  result;
    result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(rFinder, result, "bid", blockId, "inodeid", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ReplicaUnderConstruction instances with INodeID=" + inodeId + ", BlockID=" + blockId);
      aboutToAccessStorage(rFinder, params);
      result = dataAccess.findReplicaUnderConstructionByBlockId(blockId, inodeId);
      updateCache(result);
      gotFromDB(new BlockPK(blockId, inodeId), result);
      miss(rFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeId(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final long inodeId = (Long) params[0];

    List<ReplicaUnderConstruction> result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(rFinder, result, "inodeid", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ReplicaUnderConstruction instances with INodeID=" + inodeId);
      aboutToAccessStorage(rFinder, params);
      result = dataAccess.findReplicaUnderConstructionByINodeId(inodeId);
      updateCache(result);
      gotFromDB(new BlockPK(null, inodeId), result);
      miss(rFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeIds(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final long[] inodeIds = (long[]) params[0];
    if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ReplicaUnderConstruction instances with INodeIDs=" +
            StringUtils.join(", ", Arrays.stream(inodeIds).boxed().collect(Collectors.toList())));
    aboutToAccessStorage(rFinder, params);
    List<ReplicaUnderConstruction> result =
        dataAccess.findReplicaUnderConstructionByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    updateCache(result);
    miss(rFinder, result, "inodeids", Arrays.toString(inodeIds));
    return result;
  }

}
