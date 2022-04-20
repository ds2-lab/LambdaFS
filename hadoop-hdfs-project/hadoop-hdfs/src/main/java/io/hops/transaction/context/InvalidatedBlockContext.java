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
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InvalidatedBlockContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, InvalidatedBlock> {
  public static final Logger LOG = LoggerFactory.getLogger(InvalidatedBlockContext.class);

  private final InvalidateBlockDataAccess<InvalidatedBlock> dataAccess;
  private boolean allInvBlocksRead = false;

  public InvalidatedBlockContext(InvalidateBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  private ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> getReplicaCache() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null)
      return null;

    return (ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock>) instance.getNamesystem().getMetadataCacheManager().getReplicaCacheManager().getReplicaCache(this.getClass());
  }

  private InvalidatedBlock checkCache(long inodeId, long blockId, int storageId) {
    if (!EntityContext.isLocalMetadataCacheEnabled()) return null;

    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return null;

    BlockPK.ReplicaPK pk = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    return cache.getByPrimaryKey(pk);
  }

  // Uses same semantics as the `findByPK()` function.
  private InvalidatedBlock checkCacheByPk(long blockId, int storageId) {
    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return null;

    List<InvalidatedBlock> possibleReplicas = cache.getByBlockId(blockId);

    if (possibleReplicas == null) return null;

    for (InvalidatedBlock replica : possibleReplicas) {
      if (replica.getStorageId() == storageId)
        return replica;
    }

    return null;
  }

  private List<InvalidatedBlock> checkCacheByINodeId(long inodeId) {
    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByINodeId(inodeId);
  }

  private List<InvalidatedBlock> checkCacheByBlockId(long blockId) {
    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return null;

    return cache.getByBlockId(blockId);
  }

  private void updateCache(InvalidatedBlock replica) {
    if (replica == null) return;

    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return;

    cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
  }

  private void updateCache(List<InvalidatedBlock> replicas) {
    if (replicas == null) return;

    ReplicaCache<BlockPK.ReplicaPK, InvalidatedBlock> cache = getReplicaCache();
    if (cache == null) return;

    for (InvalidatedBlock replica : replicas) {
      cache.cacheEntry(new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(), replica.getStorageId()), replica);
    }
  }
  
  @Override
  public void update(InvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.update(hopInvalidatedBlock);
    if(isLogTraceEnabled()) {
      log("added-invblock", "bid", hopInvalidatedBlock.getBlockId(), "sid",
         hopInvalidatedBlock.getStorageId());
    }
  }

  @Override
  public void remove(InvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.remove(hopInvalidatedBlock);
    if(isLogTraceEnabled()) {
      log("removed-invblock", "bid", hopInvalidatedBlock.getBlockId(), "sid",
        hopInvalidatedBlock.getStorageId());
    }
  }

  @Override
  public InvalidatedBlock find(FinderType<InvalidatedBlock> finder,
      Object... params) throws TransactionContextException, StorageException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdSidAndINodeId:
        return findByPrimaryKey(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<InvalidatedBlock> findList(
      FinderType<InvalidatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case All:
        return findAll(iFinder);
      case BySid:
        return findBySid(iFinder, params);
      case ByINodeIds:
        return findByINodeIds(iFinder, params);
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
    allInvBlocksRead = false;
  }

  @Override
  InvalidatedBlock cloneEntity(InvalidatedBlock hopInvalidatedBlock) {
    return cloneEntity(hopInvalidatedBlock, hopInvalidatedBlock.getInodeId());
  }

  @Override
  InvalidatedBlock cloneEntity(InvalidatedBlock hopInvalidatedBlock,
      long inodeId) {
    return new InvalidatedBlock(hopInvalidatedBlock.getStorageId(),
        hopInvalidatedBlock.getBlockId(), inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(InvalidatedBlock hopInvalidatedBlock) {
    return new BlockPK.ReplicaPK(hopInvalidatedBlock.getBlockId(),
        hopInvalidatedBlock.getInodeId(), hopInvalidatedBlock.getStorageId());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getRemoved().isEmpty();
  }

  private InvalidatedBlock findByPrimaryKey(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    final long inodeId = (Long) params[2];

    final BlockPK.ReplicaPK key = new BlockPK.ReplicaPK(blockId, inodeId, storageId);

    InvalidatedBlock result = checkCache(blockId, inodeId, storageId);
    if (result != null) return result;

    if (contains(key) || containsByBlock(blockId) || containsByINode(inodeId)) {
      result = get(key);
      hit(iFinder, result, "bid", blockId, "sid", storageId, "inodeId",
          inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for InvalidatedBlock instance with INodeID=" + inodeId + ", BlockID=" + blockId +
              ", StorageID=" + storageId);
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvBlockByPkey(blockId, storageId, inodeId);
      gotFromDB(key, result);
      updateCache(result);
      miss(iFinder, result, "bid", blockId, "sid", storageId, "inodeId",
          inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findByBlockId(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];

    List<InvalidatedBlock> result = checkCacheByBlockId(blockId);
    if (result != null) return  result;
    result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(iFinder, result, "bid", blockId, "inodeId", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for InvalidatedBlock instances.");
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvalidatedBlocksByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId, null), result);
      updateCache(result);
      miss(iFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findByINodeId(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];

    List<InvalidatedBlock> result = checkCacheByINodeId(inodeId);
    if (result != null) return  result;

    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(iFinder, result, "inodeId", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for InvalidatedBlock instances with INodeID=" + inodeId);
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvalidatedBlocksByINodeId(inodeId);
      gotFromDB(new BlockPK(null, inodeId), result);
      updateCache(result);
      miss(iFinder, result, "inodeId", inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findAll(InvalidatedBlock.Finder iFinder)
      throws StorageCallPreventedException, StorageException {
    List<InvalidatedBlock> result = null;
    if (allInvBlocksRead) {
      result = new ArrayList<>(getAll());
      hit(iFinder, result);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for ALL InvalidatedBlock instances.");
      aboutToAccessStorage(iFinder);
      result = dataAccess.findAllInvalidatedBlocks();
      updateCache(result);
      gotFromDB(result);
      allInvBlocksRead = true;
      miss(iFinder, result);
    }
    return result;
  }

  private List<InvalidatedBlock> findBySid(
      InvalidatedBlock.Finder iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long[] blockIds = (long[]) params[0];
    final long[] inodeIds = (long[]) params[1];
    final int sid = (Integer) params[2];

    if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for InvalidatedBlock instances with SID=" + sid);

    aboutToAccessStorage(iFinder, params);
    List<InvalidatedBlock> result = dataAccess.findInvalidatedBlockByStorageId(sid);

    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, inodeIds, sid),
        result);

    updateCache(result);

    miss(iFinder, result, "bids", Arrays.toString(blockIds), "inodeIds", Arrays.toString(inodeIds), "sid", sid);
    return result;
  }

  private List<InvalidatedBlock> findByINodeIds(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long[] inodeIds = (long[]) params[0];

    if (LOG.isTraceEnabled()) LOG.trace("Going to NDB for InvalidatedBlock instances");

    aboutToAccessStorage(iFinder, params);
    List<InvalidatedBlock> result =
        dataAccess.findInvalidatedBlocksByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(iFinder, result, "inodeIds", Arrays.toString(inodeIds));

    updateCache(result);
    return result;
  }
}
