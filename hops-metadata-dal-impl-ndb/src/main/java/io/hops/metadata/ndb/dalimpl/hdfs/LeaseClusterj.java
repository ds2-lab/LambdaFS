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
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.entity.Lease;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LeaseClusterj implements TablesDef.LeaseTableDef, LeaseDataAccess<Lease> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = HOLDER_ID)
  public interface LeaseDTO {
    @PrimaryKey
    @Column(name = HOLDER_ID)
    int getHolderId();
    void setHolderId(int holder_id);
    
    @PrimaryKey
    @Column(name = HOLDER)
    String getHolder();
    void setHolder(String holder);

    @Column(name = LAST_UPDATE)
    @Index(name = "update_idx")
    long getLastUpdate();
    void setLastUpdate(long last_upd);

    @Column(name = COUNT)
    int getCount();
    void setCount(int count);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private static Log log = LogFactory.getLog(LeaseDataAccess.class);

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public Lease findByPKey(String holder, int holderId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] key = new Object[2];
    key[0] = holderId;
    key[1] = holder;
    LeaseDTO lTable = session.find(LeaseDTO.class, key);
    if (lTable != null) {
      Lease lease = createLease(lTable);
      session.release(lTable);
      return lease;
    }
    return null;
  }

  @Override
  public Lease findByHolderId(int holderId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LeaseDTO> dobj =
        qb.createQueryDefinition(LeaseDTO.class);

    HopsPredicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));

    dobj.where(pred1);

    HopsQuery<LeaseDTO> query = session.createQuery(dobj);
    query.setParameter("param1", holderId); 
    List<LeaseDTO> leaseTables = query.getResultList();

    if (leaseTables.size() > 1) {
      log.error(
          "Error in selectLeaseTableInternal: Multiple rows with same holderID");
      session.release(leaseTables);
      return null;
    } else if (leaseTables.size() == 1) {
      Lease lease = createLease(leaseTables.get(0));
      session.release(leaseTables);
      return lease;
    } else {
      log.info("No rows found for holderID:" + holderId + " in Lease table");
      return null;
    }
  }

  @Override
  public Collection<Lease> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<LeaseDTO> query = session.createQuery(
            qb.createQueryDefinition(LeaseDTO.class));    
    List<LeaseDTO> dtos = query.getResultList();
    Collection<Lease> ll = createList(dtos);
    session.release(dtos);
    return ll;
  }

  @Override
  public Collection<Lease> findByTimeLimit(long timeLimit)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaseDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("lastUpdate");
    String param = "timelimit";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate lessThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(lessThan);
    HopsQuery query = session.createQuery(dobj);
    query.setParameter(param, timeLimit);
    
    List<LeaseDTO> dtos = query.getResultList();
    Collection<Lease> ll = createList(dtos);
    session.release(dtos);
    return ll;
  }

  @Override
  public void prepare(Collection<Lease> removed, Collection<Lease> newed,
      Collection<Lease> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<LeaseDTO> changes = new ArrayList<>();
    List<LeaseDTO> deletions = new ArrayList<>();
    try {
      for (Lease l : newed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        changes.add(lTable);
      }

      for (Lease l : modified) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        changes.add(lTable);
      }

      for (Lease l : removed) {
        Object[] key = new Object[2];
        key[0] = l.getHolderId();
        key[1] = l.getHolder();
        LeaseDTO lTable = session.newInstance(LeaseDTO.class, key);
        deletions.add(lTable);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private Collection<Lease> createList(List<LeaseDTO> list) {
    Collection<Lease> finalSet = new ArrayList<>();
    for (LeaseDTO dto : list) {
      finalSet.add(createLease(dto));
    }

    return finalSet;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(LeaseDTO.class);
  }

  private Lease createLease(LeaseDTO lTable) {
    return new Lease(lTable.getHolder(), lTable.getHolderId(),
        lTable.getLastUpdate(), lTable.getCount());
  }

  private void createPersistableLeaseInstance(Lease lease, LeaseDTO lTable) {
    lTable.setHolder(lease.getHolder());
    lTable.setHolderId(lease.getHolderId());
    lTable.setLastUpdate(lease.getLastUpdate());
    lTable.setCount(lease.getCount());
  }
}
