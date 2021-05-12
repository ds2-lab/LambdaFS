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
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.entity.CorruptReplica;
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

public class CorruptReplicaClusterj implements TablesDef.CorruptReplicaTableDef,
    CorruptReplicaDataAccess<CorruptReplica> {


  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = "timestamp")
  public interface CorruptReplicaDTO {
    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    void setINodeId(long inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int storageId);
    
    @Column(name = TIMESTAMP)
    long getTimestamp();
    void setTimestamp(long timestamp);
    
    @Column(name = REASON)
    String getReason();
    void setReason(String reason);
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
  public void prepare(Collection<CorruptReplica> removed,
      Collection<CorruptReplica> newed)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    List<CorruptReplicaDTO> changes = new ArrayList<>();
    List<CorruptReplicaDTO> deletions = new ArrayList<>();
    try {
      for (CorruptReplica corruptReplica : removed) {
        CorruptReplicaDTO newInstance =
                dbSession.newInstance(CorruptReplicaDTO.class);
        createPersistable(corruptReplica, newInstance);
        deletions.add(newInstance);
      }

      for (CorruptReplica corruptReplica : newed) {
        CorruptReplicaDTO newInstance =
                dbSession.newInstance(CorruptReplicaDTO.class);
        createPersistable(corruptReplica, newInstance);
        changes.add(newInstance);
      }
      dbSession.deletePersistentAll(deletions);
      dbSession.savePersistentAll(changes);
    }finally {
      dbSession.release(deletions);
      dbSession.release(changes);
    }
  }

  @Override
  public CorruptReplica findByPk(long blockId, int sid, int inodeId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    Object[] keys = new Object[2];
    keys[0] = inodeId;
    keys[1] = blockId;
    keys[2] = sid;

    CorruptReplicaDTO corruptReplicaTable =
        dbSession.find(CorruptReplicaDTO.class, keys);
    if (corruptReplicaTable != null) {
      CorruptReplica cr = createReplica(corruptReplicaTable);
      dbSession.release(corruptReplicaTable);
      return cr;
    } else {
      return null;
    }
  }

  @Override
  public List<CorruptReplica> findAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj =
        qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setOrdering(Query.Ordering.ASCENDING, "timestamp");
    List<CorruptReplicaDTO> ibts = query.getResultList();
    List<CorruptReplica> lcr = createCorruptReplicaList(ibts);
    dbSession.release(ibts);
    return lcr;
  }

  @Override
  public List<CorruptReplica> findByBlockId(long blockId, long inodeId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj =
        qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockId"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("blockId", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    List<CorruptReplicaDTO> creplicas = query.getResultList();
    List<CorruptReplica> lcr = createCorruptReplicaList(creplicas);
    dbSession.release(creplicas);
    return lcr;
  }
  
  @Override
  public List<CorruptReplica> findByINodeId(long inodeId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    List<CorruptReplicaDTO> dtos = query.getResultList();
    List<CorruptReplica>  lcr =  createCorruptReplicaList(dtos);
    dbSession.release(dtos);
    return lcr;
  }

  @Override
  public List<CorruptReplica> findByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj =
        qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("iNodeIdParam", Longs.asList(inodeIds));
    List<CorruptReplicaDTO> dtos = query.getResultList();
    List<CorruptReplica> crl = createCorruptReplicaList(dtos);
    dbSession.release(dtos);
    return crl;
  }

  private CorruptReplica createReplica(CorruptReplicaDTO corruptReplicaTable) {
    return new CorruptReplica(corruptReplicaTable.getStorageId(),
        corruptReplicaTable.getBlockId(), corruptReplicaTable.getINodeId(), corruptReplicaTable.getReason());
  }

  private List<CorruptReplica> createCorruptReplicaList(
      List<CorruptReplicaDTO> persistables) {
    List<CorruptReplica> replicas = new ArrayList<>();
    for (CorruptReplicaDTO bit : persistables) {
      replicas.add(createReplica(bit));
    }
    return replicas;
  }

  private void createPersistable(CorruptReplica corruptReplica,
      CorruptReplicaDTO corruptReplicaTable) {
    corruptReplicaTable.setBlockId(corruptReplica.getBlockId());
    corruptReplicaTable.setStorageId(corruptReplica.getStorageId());
    corruptReplicaTable.setINodeId(corruptReplica.getInodeId());
    corruptReplicaTable.setTimestamp(System.currentTimeMillis());
    corruptReplicaTable.setReason(corruptReplica.getReason());
  }
}
