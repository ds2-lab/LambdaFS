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
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCache;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PendingBlockContext
    extends BaseReplicaContext<BlockPK, PendingBlockInfo> {

  public static final Logger LOG = LoggerFactory.getLogger(PendingBlockContext.class);

  private final PendingBlockDataAccess<PendingBlockInfo> dataAccess;
  private boolean allPendingRead = false;

  public PendingBlockContext(PendingBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  private ReplicaCache<BlockPK, PendingBlockInfo> getReplicaCache() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null)
      return null;

    return (ReplicaCache<BlockPK, PendingBlockInfo>) instance.getNamesystem().getMetadataCacheManager().getReplicaCacheManager().getReplicaCache(this.getClass());
  }

  private PendingBlockInfo checkCache(long inodeId, long blockId) {
    if (!EntityContext.areMetadataCacheReadsEnabled()) return null;

    ReplicaCache<BlockPK, PendingBlockInfo> cache = getReplicaCache();
    if (cache == null) return null;

    BlockPK pk = new BlockPK(blockId, inodeId);

    return cache.getByPrimaryKey(pk);
  }

  private List<PendingBlockInfo> checkCacheByINodeId(long inodeId) {
    ReplicaCache<BlockPK, PendingBlockInfo> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByINodeId(inodeId);
  }

  private List<PendingBlockInfo> checkCacheByBlockId(long blockId) {
    ReplicaCache<BlockPK, PendingBlockInfo> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByBlockId(blockId);
  }

  private void updateCache(PendingBlockInfo replica) {
    if (replica == null) return;

    ReplicaCache<BlockPK, PendingBlockInfo> cache = getReplicaCache();
    if (cache == null) return;

    cache.cacheEntry(new BlockPK(replica.getBlockId(), replica.getInodeId()), replica);
  }

  private void updateCache(List<PendingBlockInfo> replicas) {
    if (replicas == null) return;

    ReplicaCache<BlockPK, PendingBlockInfo> cache = getReplicaCache();
    if (cache == null) return;

    for (PendingBlockInfo replica : replicas) {
      cache.cacheEntry(new BlockPK(replica.getBlockId(), replica.getInodeId()), replica);
    }
  }
  
  @Override
  public void update(PendingBlockInfo pendingBlockInfo)
      throws TransactionContextException {
    super.update(pendingBlockInfo);
    if(isLogTraceEnabled()) {
      log("added-pending", "bid", pendingBlockInfo.getBlockId(), "numInProgress",
              pendingBlockInfo.getNumReplicas());
    }
  }

  @Override
  public void remove(PendingBlockInfo pendingBlockInfo)
      throws TransactionContextException {
    super.remove(pendingBlockInfo);
    if(isLogTraceEnabled()) {
      log("removed-pending", "bid", pendingBlockInfo.getBlockId());
    }
  }

  @Override
  public PendingBlockInfo find(FinderType<PendingBlockInfo> finder,
      Object... params) throws TransactionContextException, StorageException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    switch (pFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(pFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<PendingBlockInfo> findList(
      FinderType<PendingBlockInfo> finder, Object... params)
      throws TransactionContextException, StorageException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    switch (pFinder) {
      case All:
        return findAll(pFinder);
      case ByINodeId:
        return findByINodeId(pFinder, params);
      case ByINodeIds:
        return findByINodeIds(pFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    allPendingRead = false;
  }

  @Override
  PendingBlockInfo cloneEntity(PendingBlockInfo pendingBlockInfo) {
    return cloneEntity(pendingBlockInfo, pendingBlockInfo.getInodeId());
  }

  @Override
  PendingBlockInfo cloneEntity(PendingBlockInfo pendingBlockInfo, long inodeId) {
    return new PendingBlockInfo(pendingBlockInfo.getBlockId(), inodeId,
        pendingBlockInfo.getTimeStamp(), pendingBlockInfo.getTargets());
  }

  @Override
  BlockPK getKey(PendingBlockInfo pendingBlockInfo) {
    return new BlockPK(pendingBlockInfo.getBlockId(),
        pendingBlockInfo.getInodeId());
  }

  private PendingBlockInfo findByBlockId(PendingBlockInfo.Finder pFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];

    PendingBlockInfo result = null;
    List<PendingBlockInfo> results = checkCacheByBlockId(blockId);
    if (results != null) {
      if (results.size() > 1)
        throw new IllegalStateException("Should have only one PendingBlockInfo per block");
      if (!results.isEmpty())
        return results.get(0);
    } else {
      results = checkCacheByINodeId(inodeId);
      if (results != null) {
        if (results.size() > 1)
          throw new IllegalStateException("Should have only one PendingBlockInfo per block");
        if (!results.isEmpty())
          return results.get(0);
      }
    }

    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      List<PendingBlockInfo> pblks = getByBlock(blockId);
      if (pblks != null) {
        if (pblks.size() > 1) {
          throw new IllegalStateException("Should have only one PendingBlockInfo per block");
        }
        if (!pblks.isEmpty()) {
          result = pblks.get(0);
        }
      }
      hit(pFinder, result, "bid", blockId, "inodeid", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for PendingBlockInfo instance with INodeID=" + inodeId + ", BlockID=" + blockId);
      aboutToAccessStorage(pFinder, params);
      result = dataAccess.findByBlockAndInodeIds(blockId, inodeId);
      updateCache(result);
      gotFromDB(new BlockPK(blockId, inodeId), result);
      miss(pFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<PendingBlockInfo> findAll(PendingBlockInfo.Finder pFinder)
      throws StorageCallPreventedException, StorageException {
    List<PendingBlockInfo> result = null;
    if (allPendingRead) {
      result = new ArrayList<>(getAll());
      hit(pFinder, result);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ALL PendingBlockInfo instances");
      aboutToAccessStorage(pFinder);
      result = dataAccess.findAll();
      gotFromDB(result);
      updateCache(result);
      allPendingRead = true;
      miss(pFinder, result);
    }
    return result;
  }

  private List<PendingBlockInfo> findByINodeId(PendingBlockInfo.Finder pFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];

    List<PendingBlockInfo> result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(pFinder, result, "inodeid", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for PendingBlockInfo instances with INodeID=" + inodeId);
      aboutToAccessStorage(pFinder, params);
      result = dataAccess.findByINodeId(inodeId);
      updateCache(result);
      gotFromDB(new BlockPK(null, inodeId), result);
      miss(pFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<PendingBlockInfo> findByINodeIds(PendingBlockInfo.Finder pFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long[] inodeIds = (long[]) params[0];
    List<PendingBlockInfo> result = null;
    if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for PendingBlockInfo instances with INodeIDs=" +
            StringUtils.join(", ", Arrays.stream(inodeIds).boxed().collect(Collectors.toList())));
    aboutToAccessStorage(pFinder, params);
    result = dataAccess.findByINodeIds(inodeIds);
    updateCache(result);
    gotFromDB(BlockPK.getBlockKeys(inodeIds), result);
    miss(pFinder, result, "inodeids", Arrays.toString(inodeIds));
    return result;
  }
}
