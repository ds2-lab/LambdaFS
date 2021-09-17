/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.StorageDataAccess;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StoragesClusterj implements TablesDef.StoragesTableDef,
    StorageDataAccess<Storage> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageDTO {
    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int sid);

    @Column(name = HOST_ID)
    String getHostId();
    void setHostId(String hostId);

    @Column(name = STORAGE_TYPE)
    int getStorageType();
    void setStorageType(int storageType);
    
    @Column(name = STATE)
    String getState();
    void setState(String state);
  }

  @Override
  public void prepare(Collection<Storage> collection,
      Collection<Storage> collection1) throws StorageException {

  }

  @Override
  public void add(Storage s) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageDTO sdto = session.newInstance(StorageDTO.class);
    sdto.setStorageId(s.getStorageID());
    sdto.setHostId(s.getHostID());
    sdto.setStorageType(s.getStorageType());
    sdto.setState(s.getState());
    session.savePersistent(sdto);
    session.release(sdto);
  }

  @Override
  public Storage findByPk(int storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageDTO sdto = session.find(StorageDTO.class, storageId);
    if (sdto == null) {
      return null;
    }
    return convertAndRelease(session, sdto);
  }

  @Override
  public List<Storage> findByHostUuid(String uuid) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<StorageDTO> dobj =
        qb.createQueryDefinition(StorageDTO.class);

    HopsPredicate pred1 = dobj.get("hostId").equal(dobj.param("hostId"));

    dobj.where(pred1);

    HopsQuery<StorageDTO> query = session.createQuery(dobj);
    query.setParameter("hostId", uuid);

    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public Collection<Storage> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<StorageDTO> qdt =
        qb.createQueryDefinition(StorageDTO.class);
    HopsQuery<StorageDTO> q = session.createQuery(qdt);
    return convertAndRelease(session, q.getResultList());
  }

  /**
   * Convert a list of storageDTO's into storages
   */
  private List<Storage> convertAndRelease(HopsSession session,
      List<StorageDTO> dtos) throws StorageException {
    ArrayList<Storage> list = new ArrayList<Storage>(dtos.size());

    for (StorageDTO dto : dtos) {
      list.add(create(dto));
      session.release(dto);
    }

    return list;
  }

  private Storage convertAndRelease(HopsSession session, StorageDTO sdto)
      throws StorageException {
    Storage storage = new Storage(sdto.getStorageId(), sdto.getHostId(), sdto
        .getStorageType(), sdto.getState());
    session.release(sdto);
    return storage;
  }

  private Storage create(StorageDTO dto) {
    Storage storage = new Storage(
        dto.getStorageId(),
        dto.getHostId(),
        dto.getStorageType(),
        dto.getState());
    return storage;
  }
}
