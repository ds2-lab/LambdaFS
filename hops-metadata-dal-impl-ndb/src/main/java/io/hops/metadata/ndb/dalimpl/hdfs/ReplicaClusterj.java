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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaClusterj
    implements TablesDef.ReplicaTableDef, ReplicaDataAccess<Replica> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = "storage_idx")
  public interface ReplicaDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();

    void setStorageId(int id);

    @Column(name = BUCKET_ID)
    int getBucketId();
    void setBucketId(int hashBucket);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<Replica> findReplicasById(long blockId, long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("blockIdParam", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  
  
  @Override
  public List<Replica> findReplicasByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  

  @Override
  public List<Replica> findReplicasByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", Longs.asList(inodeIds));
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public Map<Long,Long> findBlockAndInodeIdsByStorageId(int storageId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaDTO> res = getReplicas(session, storageId);
    Map<Long,Long> map = new HashMap<>();
    for(ReplicaDTO dto : res){
      map.put(dto.getBlockId(), dto.getINodeId() );
    }
    session.release(res);
    return map;
  }
  
  @Override
  public Map<Long, Long> findBlockAndInodeIdsByStorageIdAndBucketId(
      int storageId, int bucketId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("storageId").equal(dobj.param
        ("storageIdParam"));
    HopsPredicate pred2 = dobj.get("hashBucket").equal(dobj.param
        ("bucketIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("storageIdParam", storageId);
    query.setParameter("bucketIdParam", bucketId);
  
    List<Replica> replicas = convertAndRelease(session, query.getResultList());
    Map<Long, Long> result = new HashMap<>();
    for (Replica replica: replicas){
      result.put(replica.getBlockId(), replica.getInodeId());
    }
    return result;
  }
  
  @Override
  public void prepare(Collection<Replica> removed,
      Collection<Replica> newed, Collection<Replica> modified)
      throws StorageException {
    List<ReplicaDTO> changes = new ArrayList<>();
    List<ReplicaDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    try {
      for (Replica replica : removed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        deletions.add(newInstance);
      }

      for (Replica replica : newed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }

      for (Replica replica : modified) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
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
  public Map<Long, Long> findBlockAndInodeIdsByStorageIdAndBucketIds(
      int sId, List<Integer> mismatchedBuckets) throws StorageException {
    HopsSession session = connector.obtainSession();

    Map<Long, Long> results = new ConcurrentHashMap<>();
    if (mismatchedBuckets.size() > 0){
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaDTO.class);

      for (int i = 0 ; i < mismatchedBuckets.size() ; i++){
        HopsPredicate pred1 = dobj.get("storageId").equal(dobj.param("storageIdParam"));

        HopsPredicate pred2 = dobj.get("bucketId").equal(dobj.param("bucketIdParam"));

        dobj.where(pred1.and(pred2));
        HopsQuery<ReplicaDTO> query = session.createQuery(dobj);

        query.setParameter("storageIdParam", sId);
        query.setParameter("bucketIdParam", mismatchedBuckets.get(i));

        List<Replica> replicas = convertAndRelease(session, query.getResultList());
        for (Replica replica : replicas){
          results.put(replica.getBlockId(), replica.getInodeId());
        }
      }
      return results;
    } else {
      return results;
    }
  }
  
  @Override
  public int countAllReplicasForStorageId(int sid) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME,
        String.format("%s=%d", STORAGE_ID, sid));
  }

  protected static Set<Long> getReplicas(int storageId) throws
      StorageException {
    return MySQLQueryHelper.execute(String.format("SELECT %s " +
        "FROM %s WHERE %s='%d'", BLOCK_ID, TABLE_NAME, STORAGE_ID, storageId)
        , new MySQLQueryHelper.ResultSetHandler<Set<Long>>() {
      @Override
      public Set<Long> handle(ResultSet result) throws SQLException {
        Set<Long> blocks = Sets.newHashSet();
        while (result.next()){
          blocks.add(result.getLong(BLOCK_ID));
        }
        return blocks;
      }
    });
  }

  protected static List<ReplicaClusterj.ReplicaDTO> getReplicas(
      HopsSession session, int storageId) throws StorageException {
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    return query.getResultList();
  }

  protected static List<ReplicaClusterj.ReplicaDTO> getReplicas(
      HopsSession session, int storageId, long from, int size) throws StorageException {
    while(countBlocksInWindow(storageId, from, size)==0){
      from+=size;
    }
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("storageId")));
    dobj.where(dobj.get("blockId").between(dobj.param("minBlockId"), dobj.param("maxBlockId")));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("storageId", storageId);
    query.setParameter("minBlockId", from);
    query.setParameter("maxBlockId", from + size);
    return query.getResultList();
  }

  @Override
  public boolean hasBlocksWithIdGreaterThan(int storageId, long from) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("storageId")));
    dobj.where(dobj.get("blockId").greaterEqual(dobj.param("minBlockId")));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("storageId", storageId);
    query.setParameter("minBlockId", from);
    query.setLimits(0, 1);
    List<ReplicaDTO> dtos = query.getResultList();
    boolean result = !dtos.isEmpty();
    session.release(dtos);
    return result;
  }

  @Override  
  public long findBlockIdAtIndex(int storageId, long index, int maxFetchingSize) throws StorageException{
    HopsSession session = connector.obtainSession();
    
    long startId=0;
    long nbBlocks=0;
    long prevStartId = 0;
    long prevNbBlocks = 0;
    int windowSize = maxFetchingSize * 1000;
    while (windowSize > maxFetchingSize) {
      while (nbBlocks < index) {
        prevStartId = startId;
        prevNbBlocks = nbBlocks;
        nbBlocks += countBlocksInWindow(storageId, startId, windowSize);
        startId += windowSize;

      }
      nbBlocks = prevNbBlocks;
      startId = prevStartId;
      windowSize = windowSize/10;
    }
      
    while(nbBlocks<index){
      List<ReplicaClusterj.ReplicaDTO> list = getReplicas(session, storageId, prevStartId, maxFetchingSize);
      for(ReplicaDTO dto:list){
        nbBlocks++;
        if(nbBlocks==index){
          return dto.getBlockId();
        }
      }
      startId+=maxFetchingSize;
    }
    return 0;
  }
  
  private static Long countBlocksInWindow(int storageId, long from, int size) throws
      StorageException {
    Long result =  MySQLQueryHelper.executeLongAggrQuery(String.format("SELECT count(*) " +
        "FROM %s WHERE %s='%d' and %s>='%d' and %s<=%d",TABLE_NAME, STORAGE_ID, storageId, BLOCK_ID,
        from, BLOCK_ID, from+size));
    return result;
  }
  
  private List<Replica> convertAndRelease(HopsSession session,
      List<ReplicaDTO> triplets) throws StorageException {
    List<Replica> replicas =
        new ArrayList<>(triplets.size());
    for (ReplicaDTO t : triplets) {
        replicas.add(
            new Replica(t.getStorageId(), t.getBlockId(), t.getINodeId(), t.getBucketId()));
      session.release(t);
    }
    return replicas;
  }

  private void createPersistable(Replica replica,
      ReplicaDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setINodeId(replica.getInodeId());
    newInstance.setBucketId(replica.getBucketId());
  }
}
