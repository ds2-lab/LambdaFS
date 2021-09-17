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
import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.entity.UnderReplicatedBlock;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UnderReplicatedBlockClusterj
    implements TablesDef.UnderReplicatedBlockTableDef,
    UnderReplicatedBlockDataAccess<UnderReplicatedBlock> {

  @Override
  public int countByLevel(int level) throws StorageException {
    return MySQLQueryHelper
        .countWithCriterion(TABLE_NAME, String.format("%s=%d", LEVEL, level));
  }

  @Override
  public int countLessThanALevel(int level) throws StorageException {
    return MySQLQueryHelper
        .countWithCriterion(TABLE_NAME, String.format("%s<%d", LEVEL, level));
  }

  public int countReplOneBlocks(int level) throws StorageException {
    return MySQLQueryHelper
        .countWithCriterion(TABLE_NAME, String.format("%s=%d and %s=1", LEVEL, level, EXPECTEDREPLICAS));
  }
  
  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = "level")
  public interface UnderReplicatedBlocksDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = LEVEL)
    int getLevel();

    void setLevel(int level);
    
    @Column(name = TIMESTAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);
    
    @Column(name = EXPECTEDREPLICAS)
    int getExpectedReplicas();
    
    void setExpectedReplicas(int expectedReplicas);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public UnderReplicatedBlock findByPk(long blockId, long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = inodeId;
    pk[1] = blockId;

    UnderReplicatedBlocksDTO urbt =
        session.find(UnderReplicatedBlocksDTO.class, pk);
    if (urbt == null) {
      return null;
    }
    return convertAndRelease(session, urbt);
  }

  @Override
  public void prepare(Collection<UnderReplicatedBlock> removed,
      Collection<UnderReplicatedBlock> newed,
      Collection<UnderReplicatedBlock> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UnderReplicatedBlocksDTO> changes =
        new ArrayList<>();
    List<UnderReplicatedBlocksDTO> deletions =
        new ArrayList<>();
    try {
      for (UnderReplicatedBlock urBlock : removed) {
        UnderReplicatedBlocksDTO newInstance =
            session.newInstance(UnderReplicatedBlocksDTO.class);
        createPersistable(urBlock, newInstance);
        deletions.add(newInstance);
      }

      for (UnderReplicatedBlock urBlock : newed) {
        UnderReplicatedBlocksDTO newInstance =
            session.newInstance(UnderReplicatedBlocksDTO.class);
        createPersistable(urBlock, newInstance);
        changes.add(newInstance);
      }

      for (UnderReplicatedBlock urBlock : modified) {
        UnderReplicatedBlocksDTO newInstance =
            session.newInstance(UnderReplicatedBlocksDTO.class);
        createPersistable(urBlock, newInstance);
        changes.add(newInstance);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private void createPersistable(UnderReplicatedBlock block,
      UnderReplicatedBlocksDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setLevel(block.getLevel());
    persistable.setINodeId(block.getInodeId());
    persistable.setTimestamp(System.currentTimeMillis());
    persistable.setExpectedReplicas(block.getExpectedReplicas());
  }

  private UnderReplicatedBlock convertAndRelease(HopsSession session,
      UnderReplicatedBlocksDTO bit) throws StorageException {
    UnderReplicatedBlock block =
        new UnderReplicatedBlock(bit.getLevel(), bit.getBlockId(),
            bit.getINodeId(), bit.getExpectedReplicas());
    session.release(bit);
    return block;
  }

  private List<UnderReplicatedBlock> convertAndRelease(HopsSession session,
      List<UnderReplicatedBlocksDTO> bitList) throws StorageException {
    List<UnderReplicatedBlock> blocks = new ArrayList<>();
    for (UnderReplicatedBlocksDTO bit : bitList) {
      blocks.add(convertAndRelease(session, bit));
    }
    return blocks;
  }

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<UnderReplicatedBlock> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UnderReplicatedBlocksDTO> dobj =
        qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
    HopsQuery<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
    query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<UnderReplicatedBlock> findByLevel(int level)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UnderReplicatedBlocksDTO> dobj =
        qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
    HopsPredicate pred = dobj.get("level").equal(dobj.param("level"));
    dobj.where(pred);
    HopsQuery<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
    query.setParameter("level", level);
    query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<UnderReplicatedBlock> findByLevel(int level, int offset,
      int count) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UnderReplicatedBlocksDTO> dobj =
        qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
    HopsPredicate pred = dobj.get("level").equal(dobj.param("level"));
    dobj.where(pred);
    HopsQuery<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
    query.setParameter("level", level);
    query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
    query.setLimits(offset, count);
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<UnderReplicatedBlock> findByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UnderReplicatedBlocksDTO> qdt =
        qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
    qdt.where(pred1);
    HopsQuery<UnderReplicatedBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", inodeId);
    //FIXME[M]: it throws ClusterJUserException: There is no index containing the ordering fields.
    //http://bugs.mysql.com/bug.php?id=67765
    //query.setOrdering(HopsQuery.Ordering.ASCENDING, "level", "timestamp");
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<UnderReplicatedBlock> findByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UnderReplicatedBlocksDTO> qdt =
        qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("idParam"));
    qdt.where(pred1);
    HopsQuery<UnderReplicatedBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", Longs.asList(inodeIds));
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public void removeAll() throws StorageException {
    try {
      while (countAll() != 0) {
        MysqlServerConnector.truncateTable(TABLE_NAME, 1000);
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    }
  }
}
