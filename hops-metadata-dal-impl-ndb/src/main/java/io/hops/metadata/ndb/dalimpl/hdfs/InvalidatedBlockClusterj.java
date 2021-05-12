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
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvalidatedBlockClusterj implements
    TablesDef.InvalidatedBlockTableDef,
    InvalidateBlockDataAccess<InvalidatedBlock> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = STORAGE_IDX)
  public interface InvalidateBlocksDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    void setINodeId(long inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long blockId);
    
    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int storageId);
    
    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();
    void setGenerationStamp(long generationStamp);

    @Column(name = NUM_BYTES)
    long getNumBytes();
    void setNumBytes(long numBytes);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<InvalidatedBlock> findAllInvalidatedBlocks()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    
    List<InvalidateBlocksDTO> dtos = session.createQuery(qdt).getResultList();
    List<InvalidatedBlock> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
    
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlockByStorageId(int storageId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("param")));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("param", storageId);

    List<InvalidateBlocksDTO> dtos = query.getResultList();
    List<InvalidatedBlock> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
  }
  
  @Override
  public Map<Long, Long> findInvalidatedBlockBySidUsingMySQLServer(int storageId) throws StorageException {
    return MySQLQueryHelper.execute(String.format("SELECT %s, %s "
            + "FROM %s WHERE %s='%d'", BLOCK_ID, GENERATION_STAMP, TABLE_NAME, STORAGE_ID, storageId), new MySQLQueryHelper.ResultSetHandler<Map<Long,Long>>() {
      @Override
      public Map<Long,Long> handle(ResultSet result) throws SQLException {
        Map<Long,Long> blockInodeMap = new HashMap<>();
        while (result.next()) {
          blockInodeMap.put(result.getLong(BLOCK_ID),result.getLong(GENERATION_STAMP));
        }
        return blockInodeMap;
      }
    });
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByBlockId(long bid,
      long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("blockId").equal(qdt.param("blockIdParam"));
    HopsPredicate pred2 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1.and(pred2));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("blockIdParam", bid);
    query.setParameter("iNodeIdParam", inodeId);
    
    List<InvalidateBlocksDTO> dtos = query.getResultList();
    List<InvalidatedBlock> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
    
  }
  
  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", inodeId);
    
    List<InvalidateBlocksDTO> dtos = query.getResultList();
    List<InvalidatedBlock> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", Longs.asList(inodeIds));
    
    List<InvalidateBlocksDTO> dtos = query.getResultList();
    List<InvalidatedBlock> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
  }
  
  @Override
  public InvalidatedBlock findInvBlockByPkey(long blockId, int storageId,
      long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[3];
    pk[0] = inodeId;
    pk[1] = blockId;
    pk[2] = storageId;

    InvalidateBlocksDTO invTable = session.find(InvalidateBlocksDTO.class, pk);
    if (invTable == null) {
      return null;
    }
    InvalidatedBlock ivb = createReplica(invTable);
    session.release(invTable);
    return ivb;
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksbyPKS(
      final long[] blockIds, final long[] inodesIds, final int[] storageIds)
      throws StorageException {
    int currentTableSize = countAll();
    if (currentTableSize == 0) {
      return new ArrayList<>();
    } else if (currentTableSize < inodesIds.length) {
      return findAllInvalidatedBlocks();
    }
    final List<InvalidateBlocksDTO> invBlocks =
        new ArrayList<>();
    HopsSession session = connector.obtainSession();
    try {
      for (int i = 0; i < blockIds.length; i++) {
        InvalidateBlocksDTO invTable = session
            .newInstance(InvalidateBlocksDTO.class,
                new Object[]{inodesIds[i], blockIds[i], storageIds[i]});
        invTable.setGenerationStamp(NOT_FOUND_ROW);
        invTable = session.load(invTable);
        invBlocks.add(invTable);
      }
      session.flush();
      List<InvalidatedBlock> ivbl = createList(invBlocks);
      return ivbl;
    }finally {
      session.release(invBlocks);
    }
  }

  @Override
  public void prepare(Collection<InvalidatedBlock> removed,
      Collection<InvalidatedBlock> newed, Collection<InvalidatedBlock> modified)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InvalidateBlocksDTO> changes = new ArrayList<>();
    List<InvalidateBlocksDTO> deletions = new ArrayList<>();
    try {
      for (InvalidatedBlock invBlock : newed) {
        InvalidateBlocksDTO newInstance =
            session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        changes.add(newInstance);
      }

      for (InvalidatedBlock invBlock : removed) {
        InvalidateBlocksDTO newInstance =
            session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        deletions.add(newInstance);
      }

      if (!modified.isEmpty()) {
        throw new UnsupportedOperationException("Not yet Implemented");
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(InvalidateBlocksDTO.class);
  }

  @Override
  public void removeAllByStorageId(int storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt =
        qb.createQueryDefinition(InvalidateBlocksDTO.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("param")));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("param", storageId);
    query.deletePersistentAll();
  }


  private List<InvalidatedBlock> createList(List<InvalidateBlocksDTO> dtoList) {
    List<InvalidatedBlock> list = new ArrayList<>();
    for (InvalidateBlocksDTO dto : dtoList) {
      if (dto.getGenerationStamp() != NOT_FOUND_ROW) {
        list.add(createReplica(dto));
      }
    }
    return list;
  }

  private InvalidatedBlock createReplica(InvalidateBlocksDTO invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(),
        invBlockTable.getBlockId(), invBlockTable.getGenerationStamp(),
        invBlockTable.getNumBytes(), invBlockTable.getINodeId());
  }

  private void createPersistable(InvalidatedBlock invBlock,
      InvalidateBlocksDTO newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
    newInvTable.setINodeId(invBlock.getInodeId());
  }

  @Override
  public void removeByBlockIdAndStorageId(long blockId, int storageId) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("blockId").equal(qdt.param("blockId"));
    HopsPredicate pred2 = qdt.get("storageId").equal(qdt.param("storageId"));
    qdt.where(pred1.and(pred2));

    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("blockId", blockId);
    query.setParameter("storageId", storageId);

    query.deletePersistentAll();
  }
}
