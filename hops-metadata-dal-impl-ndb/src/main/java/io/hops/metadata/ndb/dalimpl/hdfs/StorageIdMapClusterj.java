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
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.entity.StorageId;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StorageIdMapClusterj
    implements TablesDef.StorageIdMapTableDef, StorageIdMapDataAccess<StorageId> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageIdDTO {
    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);

    @Column(name = SID)
    int getSId();

    void setSId(int sId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void add(StorageId s) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageIdDTO sdto = null;
    try {
      sdto = session.newInstance(StorageIdDTO.class);
      sdto.setSId(s.getsId());
      sdto.setStorageId(s.getStorageId());
      session.savePersistent(sdto);
    }finally {
      session.release(sdto);
    }
  }

  @Override
  public StorageId findByPk(String storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageIdDTO sdto = session.find(StorageIdDTO.class, storageId);
    if (sdto == null) {
      return null;
    }
    return convertAndRelease(session, sdto);
  }

  @Override
  public Collection<StorageId> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<StorageIdDTO> qdt =
        qb.createQueryDefinition(StorageIdDTO.class);
    HopsQuery<StorageIdDTO> q = session.createQuery(qdt);
    return convertAndRelease(session, q.getResultList());
  }

  private Collection<StorageId> convertAndRelease(HopsSession session,
      List<StorageIdDTO> dtos) throws StorageException {
    List<StorageId> hopstorageId = new ArrayList<>();
    for (StorageIdDTO sdto : dtos) {
      hopstorageId.add(convertAndRelease(session, sdto));
    }
    return hopstorageId;
  }

  private StorageId convertAndRelease(HopsSession session, StorageIdDTO sdto)
      throws StorageException {
    StorageId storageId = new StorageId(sdto.getStorageId(), sdto.getSId());
    session.release(sdto);
    return storageId;
  }
}
