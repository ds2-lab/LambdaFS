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
import static io.hops.metadata.hdfs.TablesDef.UnderReplicatedBlockTableDef.TABLE_NAME;
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.hdfs.entity.ExcessReplica;
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

public class ExcessReplicaClusterj
    implements TablesDef.ExcessReplicaTableDef, ExcessReplicaDataAccess<ExcessReplica> {


  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = STORAGE_IDX)
  public interface ExcessReplicaDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    void setINodeId(long inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long storageId);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int storageId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public int countAllUniqueBlk() throws StorageException {
    return MySQLQueryHelper.countAllUnique(TABLE_NAME, BLOCK_ID);
  }
  
  @Override
  public void prepare(Collection<ExcessReplica> removed,
      Collection<ExcessReplica> newed, Collection<ExcessReplica> modified)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ExcessReplicaDTO> changes = new ArrayList<>();
    List<ExcessReplicaDTO> deletions = new ArrayList<>();
    try {
      for (ExcessReplica exReplica : newed) {
        ExcessReplicaDTO newInstance =
                session.newInstance(ExcessReplicaDTO.class);
        createPersistable(exReplica, newInstance);
        changes.add(newInstance);
      }

      for (ExcessReplica exReplica : removed) {
        ExcessReplicaDTO newInstance =
                session.newInstance(ExcessReplicaDTO.class);
        createPersistable(exReplica, newInstance);
        deletions.add(newInstance);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public List<ExcessReplica> findExcessReplicaBySid(int sid)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ExcessReplicaDTO> qdt =
        qb.createQueryDefinition(ExcessReplicaDTO.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("sid")));
    HopsQuery<ExcessReplicaDTO> query = session.createQuery(qdt);
    query.setParameter("sid", sid);
    List<ExcessReplicaDTO> dtos = query.getResultList();
    List<ExcessReplica> ler = createList(dtos);
    session.release(dtos);
    return ler;
  }

  @Override
  public List<ExcessReplica> findExcessReplicaByBlockId(long bId, long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ExcessReplicaDTO> qdt =
        qb.createQueryDefinition(ExcessReplicaDTO.class);
    HopsPredicate pred1 = qdt.get("blockId").equal(qdt.param("blockIdParam"));
    HopsPredicate pred2 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1.and(pred2));
    HopsQuery<ExcessReplicaDTO> query = session.createQuery(qdt);
    query.setParameter("blockIdParam", bId);
    query.setParameter("iNodeIdParam", inodeId);
    
    List<ExcessReplicaDTO> dtos = query.getResultList();
    List<ExcessReplica> ler = createList(dtos);
    session.release(dtos);
    return ler;
  }
  
  @Override
  public List<ExcessReplica> findExcessReplicaByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ExcessReplicaDTO> qdt =
        qb.createQueryDefinition(ExcessReplicaDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<ExcessReplicaDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", inodeId);
    
    List<ExcessReplicaDTO> dtos = query.getResultList();
    List<ExcessReplica> ler = createList(dtos);
    session.release(dtos);
    return ler;
  }

  @Override
  public List<ExcessReplica> findExcessReplicaByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ExcessReplicaDTO> qdt =
        qb.createQueryDefinition(ExcessReplicaDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<ExcessReplicaDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", Longs.asList(inodeIds));
    
    List<ExcessReplicaDTO> dtos = query.getResultList();
    List<ExcessReplica> ler = createList(dtos);
    session.release(dtos);
    return ler;
  }

  @Override
  public ExcessReplica findByPK(long bId, int sid, long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[4];
    pk[0] = inodeId;
    pk[1] = bId;
    pk[2] = sid;

    ExcessReplicaDTO invTable = session.find(ExcessReplicaDTO.class, pk);
    if (invTable == null) {
      return null;
    }
    ExcessReplica result = createReplica(invTable);
    session.release(invTable);
    return result;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      while (countAll() != 0) {
        MysqlServerConnector.truncateTable(TABLE_NAME, 1000);
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    }
  }

  private List<ExcessReplica> createList(List<ExcessReplicaDTO> list) {
    List<ExcessReplica> result = new ArrayList<>();
    for (ExcessReplicaDTO item : list) {
      result.add(createReplica(item));
    }
    return result;
  }

  private ExcessReplica createReplica(ExcessReplicaDTO exReplicaTable) {
    return new ExcessReplica(exReplicaTable.getStorageId(),
        exReplicaTable.getBlockId(), exReplicaTable.getINodeId());
  }

  private void createPersistable(ExcessReplica exReplica,
      ExcessReplicaDTO exReplicaTable) {
    exReplicaTable.setBlockId(exReplica.getBlockId());
    exReplicaTable.setStorageId(exReplica.getStorageId());
    exReplicaTable.setINodeId(exReplica.getInodeId());
  }
}

