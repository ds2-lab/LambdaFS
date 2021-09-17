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
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.CachePoolDataAccess;
import io.hops.metadata.hdfs.entity.CachePool;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CachePoolClusterJ implements TablesDef.CachePoolTableDef, CachePoolDataAccess<CachePool> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface CachePoolDTO {

    @PrimaryKey
    @Column(name = POOL_NAME)
    String getPoolName();

    void setPoolName(String poolName);

    @Column(name = OWNER_NAME)
    String getOwnerName();

    void setOwnerName(String ownerName);

    @Column(name = GROUP_NAME)
    String getGroupName();

    void setGroupName(String groupName);

    @Column(name = MODE)

    short getMode();

    void setMode(short mode);

    @Column(name = LIMIT)
    long getLimit();

    void setLimit(long limit);

    @Column(name = MAX_RELATIVE_EXPIRY_MS)
    long getMaxRelativeExpiryMs();

    void setMaxRelativeExpiryMs(long maxRelativeExpiryMs);

    @Column(name = BYTES_NEEDED)
    long getBytesNeeded();

    void setBytesNeeded(long bytesNeeded);

    @Column(name = BYTES_CACHED)
    long getBytesCached();

    void setBytesCached(long bytesCached);

    @Column(name = FILES_NEEDED)
    long getFilesNeeded();

    void setFilesNeeded(long filesNeeded);

    @Column(name = FILES_CACHED)
    long getFilesCached();

    void setFilesCached(long filesCached);
  }

  @Override
  public CachePool find(String key) throws StorageException {
    HopsSession session = connector.obtainSession();

    CachePoolDTO result = session.find(CachePoolDTO.class, key);
    if (result != null) {
      CachePool retryCacheEntry = convert(result);
      session.release(result);
      return retryCacheEntry;
    } else {
      return null;
    }
  }

  @Override
  public Collection<CachePool> findAboveName(String name) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachePoolDTO> dobj = qb.createQueryDefinition(CachePoolDTO.class);
    HopsPredicate pred1 = dobj.get("poolName").greaterThan(dobj.param("poolName"));
    dobj.where(pred1);
    HopsQuery<CachePoolDTO> query = session.createQuery(dobj);
    query.setParameter("poolName", name);
    List<CachePoolDTO> results = null;

    try {
      results = query.getResultList();
      if (results == null) {
        return null;
      } else {
        return convert(results);
      }
    } finally {
      session.release(results);
    }
  }
  
  @Override
  public Collection<CachePool> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachePoolDTO> dobj = qb.createQueryDefinition(CachePoolDTO.class);
    HopsQuery<CachePoolDTO> query = session.createQuery(dobj);
    List<CachePoolDTO> results = null;

    try {
      results = query.getResultList();
      if (results == null) {
        return null;
      } else {
        return convert(results);
      }
    } finally {
      session.release(results);
    }
  }

  @Override
  public void prepare(Collection<CachePool> removed, Collection<CachePool> modified) throws StorageException {

    List<CachePoolDTO> changes = new ArrayList<>();
    List<CachePoolDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    for (CachePool cachePool : removed) {
      CachePoolDTO newInstance = session.newInstance(CachePoolDTO.class);
      createPersistable(cachePool, newInstance);
      deletions.add(newInstance);
    }

    for (CachePool cachePool : modified) {
      CachePoolDTO newInstance = session.newInstance(CachePoolDTO.class);
      createPersistable(cachePool, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);

    session.release(deletions);
    session.release(changes);
  }

  private Collection<CachePool> convert(List<CachePoolDTO> dtos) {
    Collection<CachePool> result = new ArrayList<>(dtos.size());
    for (CachePoolDTO dto : dtos) {
      result.add(convert(dto));
    }
    return result;
  }

  private CachePool convert(CachePoolDTO result) {
    return new CachePool(result.getPoolName(), result.getOwnerName(), result.getGroupName(), result.getMode(), result.
        getLimit(), result.getMaxRelativeExpiryMs(), result.getBytesNeeded(), result.getBytesCached(), result.
        getFilesNeeded(), result.getFilesCached());
  }

  private void createPersistable(CachePool cachePool, CachePoolDTO newInstance) {
    newInstance.setPoolName(cachePool.getPoolName());
    newInstance.setBytesCached(cachePool.getBytesCached());
    newInstance.setBytesNeeded(cachePool.getBytesNeeded());
    newInstance.setFilesCached(cachePool.getFilesCached());
    newInstance.setFilesNeeded(cachePool.getFilesNeeded());
    newInstance.setGroupName(cachePool.getGroupName());
    newInstance.setLimit(cachePool.getLimit());
    newInstance.setMaxRelativeExpiryMs(cachePool.getMaxRelativeExpiryMs());
    newInstance.setMode(cachePool.getMode());
    newInstance.setOwnerName(cachePool.getOwnerName());
  }

}
