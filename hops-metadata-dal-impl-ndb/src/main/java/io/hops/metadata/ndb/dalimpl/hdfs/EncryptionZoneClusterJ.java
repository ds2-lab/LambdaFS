/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.collect.Lists;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.EncryptionZoneDataAccess;
import io.hops.metadata.hdfs.entity.EncryptionZone;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EncryptionZoneClusterJ implements TablesDef.EncryptionZones, EncryptionZoneDataAccess<EncryptionZone> {
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface EncryptionZoneDTO {
    
    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    
    void setINodeId(long inodeId);
    
    @Column(name = ZONE_INFO)
    byte[] getZoneInfo();
    
    void setZoneInfo(byte[] zoneInfo);
  }
  
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @Override
  public List<EncryptionZone> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    List<EncryptionZoneDTO> dtos = null;
    try {
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQuery<EncryptionZoneDTO> query = session.createQuery(qb.createQueryDefinition(EncryptionZoneDTO.class));
      dtos = query.getResultList();
      return convert(dtos);
    } finally {
      session.release(dtos);
    }
    
  }

  @Override
  public List<EncryptionZone> getEncryptionZoneByInodeIdBatch(List<Long> inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<EncryptionZoneDTO> dtos = Lists.newArrayListWithExpectedSize(inodeIds.size());
    try {
      for (Long inodeId : inodeIds) {
        EncryptionZoneDTO dto = session.newInstance(EncryptionZoneDTO.class, inodeId);
        session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      return convert(dtos);
    } finally {
      session.release(dtos);
    }
  }
  
  @Override
  public EncryptionZone getEncryptionZoneByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    
    EncryptionZoneDTO dto = null;
    try {
      dto = session.find(EncryptionZoneDTO.class, inodeId);
      
      return convert(dto);
    } finally {
      session.release(dto);
    }
    
  }
  
  @Override
  public void prepare(Collection<EncryptionZone> removed, Collection<EncryptionZone> newed,
      Collection<EncryptionZone> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<EncryptionZoneDTO> changes = new ArrayList<>();
    List<EncryptionZoneDTO> deletions = new ArrayList<>();
    try {
      for (EncryptionZone encryptionZone : removed) {
        EncryptionZoneDTO persistable = createPersistable(session, encryptionZone);
        deletions.add(persistable);
      }
      
      for (EncryptionZone encryptionZone : newed) {
        EncryptionZoneDTO persistable = createPersistable(session, encryptionZone);
        changes.add(persistable);
      }
      
      for (EncryptionZone encryptionZone : modified) {
        EncryptionZoneDTO persistable = createPersistable(session, encryptionZone);
        changes.add(persistable);
      }
      
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }
  
  private EncryptionZoneDTO createPersistable(HopsSession session, EncryptionZone encryptionZone)
      throws StorageException {
    EncryptionZoneDTO dto = session.newInstance(EncryptionZoneDTO.class);
    dto.setINodeId(encryptionZone.getInodeId());
    dto.setZoneInfo(encryptionZone.getZoneInfo());
    return dto;
  }
  
  private List<EncryptionZone> convert(List<EncryptionZoneDTO> dtos) {
    List<EncryptionZone> results = Lists.newArrayListWithExpectedSize(dtos.size());
    for (EncryptionZoneDTO dto : dtos) {
      results.add(convert(dto));
    }
    return results;
  }
  
  private EncryptionZone convert(EncryptionZoneDTO dto) {
    return new EncryptionZone(dto.getINodeId(), dto.getZoneInfo());
  }
}
