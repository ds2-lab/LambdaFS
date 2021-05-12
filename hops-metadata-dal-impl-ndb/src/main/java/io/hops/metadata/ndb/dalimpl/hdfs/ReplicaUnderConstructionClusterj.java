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
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.metadata.hdfs.entity.ReplicaUnderConstruction;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReplicaUnderConstructionClusterj
    implements TablesDef.ReplicaUnderConstructionTableDef,
    ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface ReplicaUcDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blkid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();

    void setStorageId(int id);

    @Column(name = STATE)
    int getState();

    void setState(int state);
    
    @Column(name = HASH_BUCKET)
    int getBucketId();
    void setBucketId(int bucketId);
    
    @Column(name = CHOSEN_AS_PRIMARY)
    byte getChosenAsPrimary();
    void setChosenAsPrimary(byte chosenAsPrimary);
    
    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();
    void setGenerationStamp(long generationStamp);
    
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<ReplicaUnderConstruction> removed,
      Collection<ReplicaUnderConstruction> newed,
      Collection<ReplicaUnderConstruction> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaUcDTO> changes = new ArrayList<>();
    List<ReplicaUcDTO> deletions = new ArrayList<>();
    try {
      for (ReplicaUnderConstruction replica : removed) {
        ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
        createPersistable(replica, newInstance);
        deletions.add(newInstance);
      }

      for (ReplicaUnderConstruction replica : newed) {
        ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }

      for (ReplicaUnderConstruction replica : modified) {
        ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }
      
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(
      long blockId, long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj =
        qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("blockIdParam", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByINodeId(
      long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj =
        qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByINodeIds(
      long[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj =
        qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", Longs.asList(inodeIds));
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  private List<ReplicaUnderConstruction> convertAndRelease(HopsSession session,
      List<ReplicaUcDTO> replicaUc) throws StorageException {
    List<ReplicaUnderConstruction> replicas =
        new ArrayList<>(replicaUc.size());
    for (ReplicaUcDTO t : replicaUc) {
      replicas.add(new ReplicaUnderConstruction(t.getState(), t.getStorageId(),
          t.getBlockId(), t.getINodeId(), t.getBucketId(), NdbBoolean.convert(t.getChosenAsPrimary()),
          t.getGenerationStamp()));
      session.release(t);
    }
    return replicas;
  }

  private void createPersistable(ReplicaUnderConstruction replica,
      ReplicaUcDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState());
    newInstance.setINodeId(replica.getInodeId());
    newInstance.setBucketId(replica.getBucketId());
    newInstance.setChosenAsPrimary(NdbBoolean.convert(replica.getChosenAsPrimary()));
    newInstance.setGenerationStamp(replica.getGenerationStamp());
  }
}
