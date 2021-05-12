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
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.metadata.hdfs.entity.CacheDirective;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheDirectiveClusterj implements TablesDef.CacheDirectiveTableDef,
    CacheDirectiveDataAccess<CacheDirective> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private CacheDirectivePathClusterj pathDA = new CacheDirectivePathClusterj();
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface CacheDirectiveDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @Column(name = REPLICATION)
    short getReplication();

    void setReplication(short replication);

    @Column(name = EXPIRYTIME)

    long getExpiryTime();

    void setExpiryTime(long expiryTime);

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

    @Column(name = POOL)
    String getPool();

    void setPool(String pool);
  }

  @Override
  public CacheDirective find(long key) throws StorageException {
    HopsSession session = connector.obtainSession();
    CacheDirectiveDTO result = session.find(CacheDirectiveDTO.class, key);
    if (result != null) {
      CacheDirective retryCacheEntry = convert(result);
      session.release(result);
      return retryCacheEntry;
    } else {
      return null;
    }
  }

  @Override
  public Collection<CacheDirective> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CacheDirectiveDTO> dobj = qb.createQueryDefinition(CacheDirectiveDTO.class);
    HopsQuery<CacheDirectiveDTO> query = session.createQuery(dobj);

    List<CacheDirectiveDTO> dtos = query.getResultList();
    Collection<CacheDirective> directives = convert(dtos);
    session.release(dtos);
    return directives;
  }
  
  @Override
  public Collection<CacheDirective> findByPool(final String pool) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CacheDirectiveDTO> dobj = qb.createQueryDefinition(CacheDirectiveDTO.class);
    HopsPredicate pred1 = dobj.get("pool").equal(dobj.param("pool"));
    dobj.where(pred1);
    HopsQuery<CacheDirectiveDTO> query = session.createQuery(dobj);
    query.setParameter("pool", pool);
    List<CacheDirectiveDTO> results = null;

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
  public Collection<CacheDirective> findByIdAndPool(long id, final String pool) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CacheDirectiveDTO> dobj = qb.createQueryDefinition(CacheDirectiveDTO.class);
    HopsPredicate pred1 = dobj.get("id").greaterEqual(dobj.param("id"));
    if(pool!=null){
      HopsPredicate pred2 = dobj.get("pool").equal(dobj.param("pool"));
      pred1 = pred1.and(pred2);
    }
    dobj.where(pred1);
    HopsQuery<CacheDirectiveDTO> query = session.createQuery(dobj);
    query.setParameter("id", id);
    if(pool!=null){
      query.setParameter("pool", pool);
    }
    List<CacheDirectiveDTO> results = null;

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
  public void prepare(Collection<CacheDirective> removed,
      Collection<CacheDirective> modified) throws StorageException {

    List<CacheDirectiveDTO> changes = new ArrayList<>();
    List<CacheDirectiveDTO> deletions = new ArrayList<>();
    Map<Long, String> pathToAdd = new HashMap<>(modified.size());
    HopsSession session = connector.obtainSession();
    for (CacheDirective cacheDirective : removed) {
      CacheDirectiveDTO newInstance = session.newInstance(CacheDirectiveDTO.class);
      createPersistable(cacheDirective, newInstance);
      deletions.add(newInstance);
      //path deletion is done by foreignkey.
    }

    for (CacheDirective cacheDirective : modified) {
      CacheDirectiveDTO newInstance = session.newInstance(CacheDirectiveDTO.class);
      createPersistable(cacheDirective, newInstance);
      changes.add(newInstance);
      pathToAdd.put(cacheDirective.getId(), cacheDirective.getPath());
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);

    session.release(deletions);
    session.release(changes);
    
    pathDA.add(pathToAdd);
  }

  private Collection<CacheDirective> convert(List<CacheDirectiveDTO> dtos) throws StorageException {
    Collection<CacheDirective> result = new ArrayList<>(dtos.size());
    for (CacheDirectiveDTO dto : dtos) {
      result.add(convert(dto));
    }
    return result;
  }

  private CacheDirective convert(CacheDirectiveDTO result) throws StorageException{
    return new CacheDirective(result.getId(), pathDA.find(result.getId()), result.getReplication(), result.getExpiryTime(),
        result.getBytesNeeded(), result.getBytesCached(), result.getFilesNeeded(), result.getFilesCached(), result.
        getPool());
  }

  private void createPersistable(CacheDirective cacheDirective, CacheDirectiveDTO newInstance) {
    newInstance.setId(cacheDirective.getId());
    newInstance.setBytesCached(cacheDirective.getBytesCached());
    newInstance.setBytesNeeded(cacheDirective.getBytesNeeded());
    newInstance.setExpiryTime(cacheDirective.getExpiryTime());
    newInstance.setFilesCached(cacheDirective.getFilesCached());
    newInstance.setFilesNeeded(cacheDirective.getFilesNeeded());
    newInstance.setPool(cacheDirective.getPool());
    newInstance.setReplication(cacheDirective.getReplication());
  }
}
