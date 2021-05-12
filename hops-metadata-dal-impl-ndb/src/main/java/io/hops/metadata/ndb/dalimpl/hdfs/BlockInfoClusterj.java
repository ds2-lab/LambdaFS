/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.entity.BlockInfo;
import io.hops.metadata.hdfs.entity.BlockLookUp;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class BlockInfoClusterj
        implements TablesDef.BlockInfoTableDef, BlockInfoDataAccess<BlockInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface BlockInfoDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long iNodeID);

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int idx);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numbytes);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long genstamp);

    @Column(name = BLOCK_UNDER_CONSTRUCTION_STATE)
    int getBlockUCState();

    void setBlockUCState(int BlockUCState);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long ts);

    @Column(name = PRIMARY_NODE_INDEX)
    int getPrimaryNodeIndex();

    void setPrimaryNodeIndex(int replication);

    @Column(name = BLOCK_RECOVERY_ID)
    long getBlockRecoveryId();

    void setBlockRecoveryId(long recoveryId);
    
    @Column(name = TRUNCATE_BLOCK_NUM_BYTES)
    long getTruncateBlockNumBytes();

    void setTruncateBlockNumBytes(long numBytes);
    
    @Column(name = TRUNCATE_BLOCK_GENERATION_STAMP)
    long getTruncateBlockGenerationBlock();

    void setTruncateBlockGenerationBlock(long generationStamp);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public int countAllCompleteBlocks() throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME,
        String.format("%s=%d", BLOCK_UNDER_CONSTRUCTION_STATE, 0));
  }

  @Override
  public void prepare(Collection<BlockInfo> removed, Collection<BlockInfo> news,
          Collection<BlockInfo> modified) throws StorageException {
    List<BlockInfoDTO> blkChanges = new ArrayList<>();
    List<BlockInfoDTO> blkDeletions = new ArrayList<>();
    List<BlockLookUpClusterj.BlockLookUpDTO> luChanges =
        new ArrayList<>();
    List<BlockLookUpClusterj.BlockLookUpDTO> luDeletions =
        new ArrayList<>();

    HopsSession session = connector.obtainSession();
    try {
      for (BlockInfo block : removed) {
        Object[] pk = new Object[2];
        pk[0] = block.getInodeId();
        pk[1] = block.getBlockId();

        BlockInfoClusterj.BlockInfoDTO bTable =
                session.newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
        blkDeletions.add(bTable);

        //delete the row from persistance table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = session
                .newInstance(BlockLookUpClusterj.BlockLookUpDTO.class,
                        block.getBlockId());
        luDeletions.add(lookupDTO);
      }

      for (BlockInfo block : news) {
        BlockInfoClusterj.BlockInfoDTO bTable =
                session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO =
                session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(
                new BlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }

      for (BlockInfo block : modified) {
        BlockInfoClusterj.BlockInfoDTO bTable =
                session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO =
                session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(
                new BlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }
      session.deletePersistentAll(blkDeletions);
      session.deletePersistentAll(luDeletions);
      session.savePersistentAll(blkChanges);
      session.savePersistentAll(luChanges);
    }finally {
      session.release(blkDeletions);
      session.release(luDeletions);
      session.release(blkChanges);
      session.release(luChanges);
    }
  }

  @Override
  public BlockInfo findById(long blockId, long inodeId) throws StorageException {
    Object[] pk = new Object[2];
    pk[0] = inodeId;
    pk[1] = blockId;

    HopsSession session = connector.obtainSession();
    BlockInfoClusterj.BlockInfoDTO bit =
            session.find(BlockInfoClusterj.BlockInfoDTO.class, pk);
    if (bit == null) {
      return null;
    }

    BlockInfo bi = createBlockInfo(bit);
    session.release(bit);

    return bi;
  }

  @Override
  public List<BlockInfo> findByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockInfoDTO> dobj =
            qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<BlockInfoDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", inodeId);
    List<BlockInfoDTO> dtos = query.getResultList();
    List<BlockInfo> lbis = createBlockInfoList(dtos);
    session.release(dtos);
    return lbis;
  }

  @Override
  public List<BlockInfo> findByInodeIds(long[] inodeIds)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj =
            qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", Longs.asList(inodeIds));

    List<BlockInfoDTO> biDtos = query.getResultList();
    List<BlockInfo> lbis = createBlockInfoList(biDtos);
    session.release(biDtos);
    return lbis;
  }

  public BlockInfo scanByBlockId(long blockId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj =
            qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
    dobj.where(pred1);
    HopsQuery<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
    query.setParameter("blockIdParam", blockId);
    List<BlockInfoDTO> biDtos = query.getResultList();
    BlockInfo bi = createBlockInfo(biDtos.get(0));
    session.release(biDtos);
    return bi;
  }

  @Override
  public List<BlockInfo> findAllBlocks() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj =
            qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
    HopsQuery<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);

    List<BlockInfoDTO> biDtos = query.getResultList();
    List<BlockInfo> lbis = createBlockInfoList(biDtos);
    session.release(biDtos);
    return lbis;
  }

  @Override
  public List<BlockInfo> findBlockInfosByStorageId(int storageId)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaClusterj.ReplicaDTO> replicas = ReplicaClusterj.getReplicas(session, storageId);
    long[] blockIds = new long[replicas.size()];
    long[] inodeIds = new long[replicas.size()];
    for (int i = 0; i < blockIds.length; i++) {
      blockIds[i] = replicas.get(i).getBlockId();
      inodeIds[i] = replicas.get(i).getINodeId();
    }
    List<BlockInfo> ret = readBlockInfoBatch(session, inodeIds, blockIds);
    session.release(replicas);
    return ret;
  }

  @Override
  public List<BlockInfo> findBlockInfosByStorageId(int storageId, long from, int size)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaClusterj.ReplicaDTO> replicas = ReplicaClusterj.getReplicas(session, storageId, from, size);
    long[] blockIds = new long[replicas.size()];
    long[] inodeIds = new long[replicas.size()];
    for (int i = 0; i < blockIds.length; i++) {
      blockIds[i] = replicas.get(i).getBlockId();
      inodeIds[i] = replicas.get(i).getINodeId();
    }
    List<BlockInfo> ret = readBlockInfoBatch(session, inodeIds, blockIds);
    session.release(replicas);
    return ret;
  }
  
  @Override
  public List<BlockInfo> findBlockInfosBySids(List<Integer> sids) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaClusterj.ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);

    HopsPredicate pred1 = dobj.get("storageId").in(dobj.param("sids"));
    dobj.where(pred1);

    HopsQuery<ReplicaClusterj.ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("sids", sids);

    List<ReplicaClusterj.ReplicaDTO> replicas = query.getResultList();

    long[] blockIds = new long[replicas.size()];
    long[] inodeIds = new long[replicas.size()];
    for (int i = 0; i < blockIds.length; i++) {
      blockIds[i] = replicas.get(i).getBlockId();
      inodeIds[i] = replicas.get(i).getINodeId();
    }

    List<BlockInfo> ret = readBlockInfoBatch(session, inodeIds, blockIds);
    session.release(replicas);
    return ret;
  }

  @Override
  public Set<Long> findINodeIdsByStorageId(int storageId)
          throws StorageException {
    return ReplicaClusterj.getReplicas(storageId);
  }

  @Override
  public List<BlockInfo> findByIds(long[] blockIds, long[] inodeIds)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<BlockInfo> blks = readBlockInfoBatch(session, inodeIds, blockIds);
    return blks;
  }

  public boolean existsOnAnyStorage(long inodeId, long blockId, List<Integer> sids) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    
    List<ReplicaClusterj.ReplicaDTO> dtos = new ArrayList<>();
    for(Integer sid: sids){
      Object[] pk = new Object[]{inodeId, blockId, sid};
      ReplicaClusterj.ReplicaDTO dto = session.newInstance(ReplicaClusterj.ReplicaDTO.class, pk);
      dto.setBucketId(NOT_FOUND_ROW);
      dto = session.load(dto);
      dtos.add(dto);
      
    }
    session.flush();
    boolean exist = false;
    for(ReplicaClusterj.ReplicaDTO dto: dtos){
      if(dto.getBucketId()!=NOT_FOUND_ROW){
        exist = true;
        break;
      }
    }
    session.release(dtos);

    return exist;
  }

  private List<BlockInfo> readBlockInfoBatch(final HopsSession session,
          final long[] inodeIds, final long[] blockIds) throws StorageException {
    final List<BlockInfoClusterj.BlockInfoDTO> bdtos =
        new ArrayList<>();
    try {
      for (int i = 0; i < blockIds.length; i++) {
        Object[] pk = new Object[]{inodeIds[i], blockIds[i]};
        BlockInfoClusterj.BlockInfoDTO bdto =
                session.newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
        bdto.setBlockIndex(NOT_FOUND_ROW);
        bdto = session.load(bdto);
        bdtos.add(bdto);
      }
      session.flush();
      List<BlockInfo> lbis = createBlockInfoList(bdtos);
      return lbis;
    }finally{
      session.release(bdtos);
    }
  }

  private List<BlockInfo> createBlockInfoList(
          List<BlockInfoClusterj.BlockInfoDTO> bitList) {
    List<BlockInfo> list = new ArrayList<>();
    if (bitList != null) {
      for (BlockInfoClusterj.BlockInfoDTO blockInfoDTO : bitList) {
        if (blockInfoDTO.getBlockIndex() != NOT_FOUND_ROW) {
          list.add(createBlockInfo(blockInfoDTO));
        }
      }
    }
    return list;
  }

  private BlockInfo createBlockInfo(BlockInfoClusterj.BlockInfoDTO bDTO) {
    BlockInfo hopBlockInfo =
            new BlockInfo(bDTO.getBlockId(), bDTO.getBlockIndex(),
            bDTO.getINodeId(), bDTO.getNumBytes(), bDTO.getGenerationStamp(),
            bDTO.getBlockUCState(), bDTO.getTimestamp(),
            bDTO.getPrimaryNodeIndex(), bDTO.getBlockRecoveryId(),
            bDTO.getTruncateBlockNumBytes(), bDTO.getTruncateBlockGenerationBlock());
    return hopBlockInfo;
  }

  private void createPersistable(BlockInfo block,
          BlockInfoClusterj.BlockInfoDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setNumBytes(block.getNumBytes());
    persistable.setGenerationStamp(block.getGenerationStamp());
    persistable.setINodeId(block.getInodeId());
    persistable.setTimestamp(block.getTimeStamp());
    persistable.setBlockIndex(block.getBlockIndex());
    persistable.setBlockUCState(block.getBlockUCState());
    persistable.setPrimaryNodeIndex(block.getPrimaryNodeIndex());
    persistable.setBlockRecoveryId(block.getBlockRecoveryId());
    persistable.setTruncateBlockNumBytes(block.getTruncateBlockNumBytes());
    persistable.setTruncateBlockGenerationBlock(block.getTruncateBlockGenerationStamp());
  }
}
