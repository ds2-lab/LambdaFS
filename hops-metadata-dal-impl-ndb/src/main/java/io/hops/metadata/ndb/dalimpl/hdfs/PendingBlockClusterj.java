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
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.metadata.hdfs.entity.PendingBlockInfo;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PendingBlockClusterj
    implements TablesDef.PendingBlockTableDef, PendingBlockDataAccess<PendingBlockInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface PendingBlockDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blockId);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);

    @Column(name = TARGET)
    String getTarget();

    void setTarget(String target);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return MySQLQueryHelper.countUniqueWithCriterion(TABLE_NAME, String.format("%s, %s", INODE_ID, BLOCK_ID),
        String.format("%s>%d", TIME_STAMP, timeLimit));
  }
  
  @Override
  public List<PendingBlockInfo> findByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", inodeId);

    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<PendingBlockInfo> findByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", Longs.asList(inodeIds));

    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public void prepare(Collection<PendingBlockInfo> removed,
      Collection<PendingBlockInfo> newed, Collection<PendingBlockInfo> modified)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<PendingBlockDTO> changes = new ArrayList<>();
    List<PendingBlockDTO> deletions = new ArrayList<>();
    try {
      for (PendingBlockInfo p : newed) {
        changes.addAll(createPersistableHopPendingBlockInfo(p, session));
      }
      for (PendingBlockInfo p : modified) {
        PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
        changes.addAll(createPersistableHopPendingBlockInfo(p, session));
      }

      for (PendingBlockInfo p : removed) {
        PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
        deletions.addAll(createPersistableHopPendingBlockInfo(p, session));
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public PendingBlockInfo findByBlockAndInodeIds(long blockId, long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> dobj = qb.createQueryDefinition(PendingBlockDTO.class);
    HopsPredicate pred = dobj.get("iNodeId").equal(dobj.param("iNodeId"));
    dobj.where(pred);
    pred = dobj.get("blockId").equal(dobj.param("blockId"));
    dobj.where(pred);
    HopsQuery<PendingBlockDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeId", inodeId);
    query.setParameter("blockId", blockId);
    List<PendingBlockDTO> dtos = query.getResultList();
    if (dtos == null || dtos.isEmpty()) {
      return null;
    }
    return convertAndRelease(session, dtos, blockId, inodeId, dtos.get(0).getTimestamp());
  }

  @Override
  public List<PendingBlockInfo> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<PendingBlockDTO> query =
        session.createQuery(qb.createQueryDefinition(PendingBlockDTO.class));
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<PendingBlockInfo> findByTimeLimitLessThan(long timeLimit)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);
    HopsPredicateOperand predicateOp = qdt.get("timestamp");
    String paramName = "timelimit";
    HopsPredicateOperand param = qdt.param(paramName);
    HopsPredicate lessThan = predicateOp.lessThan(param);
    qdt.where(lessThan);
    HopsQuery query = session.createQuery(qdt);
    query.setParameter(paramName, timeLimit);
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(PendingBlockDTO.class);
  }

  private List<PendingBlockInfo> convertAndRelease(HopsSession session,
      Collection<PendingBlockDTO> dtos) throws StorageException {
    Map<Long, Map<Long, List<PendingBlockDTO>>> pendingBlocks = new HashMap<>();
    for (PendingBlockDTO dto : dtos) {
      Map<Long, List<PendingBlockDTO>> inodePendingBlocks = pendingBlocks.get(dto.getINodeId());
      if (inodePendingBlocks == null) {
        inodePendingBlocks = new HashMap<>();
        pendingBlocks.put(dto.getINodeId(), inodePendingBlocks);
      }
      List<PendingBlockDTO> pending = inodePendingBlocks.get(dto.getBlockId());
      if (pending == null) {
        pending = new ArrayList<>();
        inodePendingBlocks.put(dto.getBlockId(), pending);
      }
      pending.add(dto);
    }
    List<PendingBlockInfo> list = new ArrayList<>();
    for (Map<Long, List<PendingBlockDTO>> inodePendingBlocks : pendingBlocks.values()) {
      for (List<PendingBlockDTO> pending : inodePendingBlocks.values()) {
        list.add(convertAndRelease(session, pending, pending.get(0).getBlockId(), pending.get(0).getINodeId(), pending.
            get(0).getINodeId()));
      }
    }
    return list;
  }

  private PendingBlockInfo convertAndRelease(HopsSession session,
      List<PendingBlockDTO> pendingTables, long blockId, long nodeId, long timestamp) throws StorageException {
    List<String> targets = new ArrayList<>();
    for(PendingBlockDTO pendingTable : pendingTables){
      if(pendingTable.getTarget()!=null && !pendingTable.getTarget().isEmpty()){
        targets.add(pendingTable.getTarget());
      }
    }
   PendingBlockDTO pendingTable = pendingTables.get(0);
    PendingBlockInfo pendingBlockInfo =  new PendingBlockInfo(blockId,nodeId, timestamp, targets);
    session.release(pendingTables);
    return pendingBlockInfo;
  }

  private List<PendingBlockDTO> createPersistableHopPendingBlockInfo(PendingBlockInfo pendingBlock, HopsSession session) throws StorageException {
    List<PendingBlockDTO> persistables = new ArrayList<>();
    if (pendingBlock.getTargets() == null || pendingBlock.getTargets().size() == 0) {
      PendingBlockDTO persistable = session.newInstance(PendingBlockDTO.class);
      persistable.setBlockId(pendingBlock.getBlockId());
      persistable.setTimestamp(pendingBlock.getTimeStamp());
      persistable.setINodeId(pendingBlock.getInodeId());
      persistable.setTarget("");
      persistables.add(persistable);
    } else {
      for (String dnId : pendingBlock.getTargets()) {
        PendingBlockDTO persistable = session.newInstance(PendingBlockDTO.class);
        persistable.setBlockId(pendingBlock.getBlockId());
        persistable.setTarget(dnId);
        persistable.setTimestamp(pendingBlock.getTimeStamp());
        persistable.setINodeId(pendingBlock.getInodeId());
        persistables.add(persistable);
      }
    }
    return persistables;
  }
}
