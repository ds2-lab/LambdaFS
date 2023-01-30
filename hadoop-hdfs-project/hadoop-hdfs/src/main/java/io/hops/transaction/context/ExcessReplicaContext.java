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
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.hdfs.entity.ExcessReplica;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCache;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExcessReplicaContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, ExcessReplica> {
  public static final Logger LOG = LoggerFactory.getLogger(ExcessReplicaContext.class);

  ExcessReplicaDataAccess<ExcessReplica> dataAccess;

  public ExcessReplicaContext(ExcessReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  private ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> getReplicaCache() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null)
      return null;

    return (ReplicaCache<BlockPK.ReplicaPK, ExcessReplica>) instance.getNamesystem().getMetadataCacheManager().getReplicaCacheManager().getReplicaCache(this.getClass());
  }

  private ExcessReplica checkCache(long inodeId, long blockId, int storageId) {
    if (!EntityContext.areMetadataCacheReadsEnabled()) return null;

    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return null;

    BlockPK.ReplicaPK pk = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    return cache.getByPrimaryKey(pk);
  }

  // Uses same semantics as the `findByPK()` function.
  private ExcessReplica checkCacheByPk(long blockId, int storageId) {
    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return null;

    List<ExcessReplica> possibleReplicas = cache.getByBlockId(blockId);

    if (possibleReplicas == null) return null;

    for (ExcessReplica replica : possibleReplicas) {
      if (replica.getStorageId() == storageId)
        return replica;
    }

    return null;
  }

  private List<ExcessReplica> checkCacheByINodeId(long inodeId) {
    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByINodeId(inodeId);
  }

  private List<ExcessReplica> checkCacheByBlockId(long blockId) {
    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByBlockId(blockId);
  }

  private void updateCache(ExcessReplica replica) {
    if (replica == null) return;

    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return;

    cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
  }

  private void updateCache(List<ExcessReplica> replicas) {
    if (replicas == null) return;

    ReplicaCache<BlockPK.ReplicaPK, ExcessReplica> cache = getReplicaCache();
    if (cache == null) return;

    for (ExcessReplica replica : replicas) {
      cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
    }
  }
  
  @Override
  public void update(ExcessReplica hopExcessReplica)
      throws TransactionContextException {
    super.update(hopExcessReplica);
    if(isLogTraceEnabled()) {
      log("added-excess", "bid", hopExcessReplica.getBlockId(), "sid",
              hopExcessReplica.getStorageId());
    }
  }

  @Override
  public void remove(ExcessReplica hopExcessReplica)
      throws TransactionContextException {
    super.remove(hopExcessReplica);
    if(isLogTraceEnabled()) {
      log("removed-excess", "bid", hopExcessReplica.getBlockId(), "sid",
              hopExcessReplica.getStorageId());
    }
  }

  @Override
  public ExcessReplica find(FinderType<ExcessReplica> finder, Object... params)
      throws TransactionContextException, StorageException {
    ExcessReplica.Finder eFinder = (ExcessReplica.Finder) finder;
    switch (eFinder) {
      case ByBlockIdSidAndINodeId:
        return findByPrimaryKey(eFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<ExcessReplica> findList(FinderType<ExcessReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    ExcessReplica.Finder eFinder = (ExcessReplica.Finder) finder;
    switch (eFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(eFinder, params);
      case ByINodeId:
        return findByINodeId(eFinder, params);
      case ByINodeIds:
        return findByINodeIds(eFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  ExcessReplica cloneEntity(ExcessReplica hopExcessReplica) {
    return cloneEntity(hopExcessReplica, hopExcessReplica.getInodeId());
  }

  @Override
  ExcessReplica cloneEntity(ExcessReplica hopExcessReplica, long inodeId) {
    return new ExcessReplica(hopExcessReplica.getStorageId(),
        hopExcessReplica.getBlockId(), inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(ExcessReplica hopExcessReplica) {
    return new BlockPK.ReplicaPK(hopExcessReplica.getBlockId(),
        hopExcessReplica.getInodeId(), hopExcessReplica.getStorageId());
  }

  private ExcessReplica findByPrimaryKey(ExcessReplica.Finder eFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    final long inodeId = (Long) params[2];
    final BlockPK.ReplicaPK key = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    ExcessReplica result = checkCache(inodeId, blockId, storageId);
    if (result != null) return result;

    if (contains(key) || containsByINode(inodeId) || containsByBlock(blockId)) {
      result = get(key);
      hit(eFinder, result, "bid", blockId, "uuid", storageId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ExcessReplica instances with INodeID=" + inodeId + ", BlockID=" + blockId +
              ", StorageID=" + storageId);
      aboutToAccessStorage(eFinder, params);
      result = dataAccess.findByPK(blockId, storageId, inodeId);
      gotFromDB(key, result);
      miss(eFinder, result, "bid", blockId, "sid", storageId);
      updateCache(result);
    }
    return result;
  }

  private List<ExcessReplica> findByBlockId(ExcessReplica.Finder eFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];

    List<ExcessReplica> result = checkCacheByBlockId(blockId);
    if (result != null) return  result;
    result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(eFinder, result, "bid", blockId, "inodeId", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ExcessReplica instances with INodeID=" + inodeId + ", BlockID=" + blockId);
      aboutToAccessStorage(eFinder, params);
      result = dataAccess.findExcessReplicaByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId, null), result);
      updateCache(result);
      miss(eFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private List<ExcessReplica> findByINodeId(ExcessReplica.Finder eFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];

    List<ExcessReplica> result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(eFinder, result, "inodeId", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ExcessReplica instances with INodeID=" + inodeId);
      aboutToAccessStorage(eFinder, params);
      result = dataAccess.findExcessReplicaByINodeId(inodeId);
      gotFromDB(new BlockPK(null, inodeId), result);
      miss(eFinder, result, "inodeId", inodeId);
      updateCache(result);
    }
    return result;
  }

  private List<ExcessReplica> findByINodeIds(ExcessReplica.Finder eFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long[] inodeIds = (long[]) params[0];
    if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ExcessReplica instances with INodeIDs=" +
            StringUtils.join(", ", Arrays.stream(inodeIds).boxed().collect(Collectors.toList())));
    aboutToAccessStorage(eFinder, params);
    List<ExcessReplica> result =
        dataAccess.findExcessReplicaByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(eFinder, result, "inodeIds", Arrays.toString(inodeIds));
    updateCache(result);
    return result;
  }
}
