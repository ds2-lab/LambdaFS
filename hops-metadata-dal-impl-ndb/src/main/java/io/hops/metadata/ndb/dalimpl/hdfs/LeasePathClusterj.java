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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LeasePathClusterj
    implements TablesDef.LeasePathTableDef, LeasePathDataAccess<LeasePath> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = HOLDER_ID)
  public interface LeasePathsDTO {
    @PrimaryKey  
    @Column(name = HOLDER_ID)
    int getHolderId();
    void setHolderId(int holder_id);

    @PrimaryKey
    @Column(name = PATH)
    @Index(name = "path_idx")
    String getPath();
    void setPath(String path);

    @Column(name = LAST_BLOCK_ID)
    long getLastBlockId();
    void setLastBlockId(long lastBlockId);

    @Column(name = PENULTIMATE_BLOCK_ID)
    long getPenultimateBlockId();
    void setPenultimateBlockId(long penultimateBlockId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<LeasePath> removed,
      Collection<LeasePath> newed, Collection<LeasePath> modified)
      throws StorageException {
    List<LeasePathsDTO> changes = new ArrayList<>();
    List<LeasePathsDTO> deletions = new ArrayList<>();
    HopsSession dbSession = connector.obtainSession();
    try {
      for (LeasePath lp : newed) {
        LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        changes.add(lTable);
      }

      for (LeasePath lp : modified) {
        LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        changes.add(lTable);
      }

      for (LeasePath lp : removed) {
        Object[] key = new Object[2];
        key[0] = lp.getHolderId();
        key[1] = lp.getPath();
        LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class, key);
        deletions.add(lTable);
      }
      dbSession.deletePersistentAll(deletions);
      dbSession.savePersistentAll(changes);
    }finally {
      dbSession.release(deletions);
      dbSession.release(changes);
    }
  }

  @Override
  public Collection<LeasePath> findByHolderId(int holderId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeasePathsDTO> dobj =
        qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
    dobj.where(pred1);
    HopsQuery<LeasePathsDTO> query = dbSession.createQuery(dobj);
    query.setParameter("param1", holderId);
    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public LeasePath findByPath(String path) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeasePathsDTO> dobj =
        qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred1 = dobj.get("path").equal(dobj.param("param1"));
    dobj.where(pred1);
    HopsQuery<LeasePathsDTO> query = dbSession.createQuery(dobj);
    query.setParameter("param1", path);

    List<LeasePathsDTO> dtos = null;
    try {
      dtos = query.getResultList();
      if (dtos == null || dtos.isEmpty()) {
        return null;
      } else if (dtos.size() == 1) {
        LeasePath lp = createLeasePath(dtos.get(0));
        return lp;
      } else {
        throw new StorageException("Found more than one path");
      }
    }finally {
      dbSession.release(dtos);
    }
  }

  @Override
  public Collection<LeasePath> findByPrefix(String prefix)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("path");
    String param = "prefix";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate like = propertyPredicate.like(propertyLimit);
    dobj.where(like);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, prefix + "%");
    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public Collection<LeasePath> findAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQuery query = dbSession.createQuery(
            qb.createQueryDefinition(LeasePathsDTO.class));    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    dbSession.deletePersistentAll(LeasePathsDTO.class);
  }

  private List<LeasePath> createList(Collection<LeasePathsDTO> dtos) {
    List<LeasePath> list = new ArrayList<>();
    for (LeasePathsDTO leasePathsDTO : dtos) {
      list.add(createLeasePath(leasePathsDTO));
    }
    return list;
  }

  private LeasePath createLeasePath(LeasePathsDTO leasePathTable) {
    return new LeasePath(leasePathTable.getPath(),
        leasePathTable.getHolderId(), leasePathTable.getLastBlockId(),
        leasePathTable.getPenultimateBlockId());
  }

  private void createPersistableLeasePathInstance(LeasePath lp,
      LeasePathsDTO lTable) {
    lTable.setHolderId(lp.getHolderId());
    lTable.setPath(lp.getPath());
    lTable.setLastBlockId(lp.getLastBlockId());
    lTable.setPenultimateBlockId(lp.getPenultimateBlockId());
  }
}
