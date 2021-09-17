/*
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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AceClusterJ implements TablesDef.AcesTableDef, AceDataAccess<Ace> {
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface AceDto {
    
    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();
    void setInodeId(long inodeId);
  
    @PrimaryKey
    @Column(name = INDEX)
    int getIndex();
    
    void setIndex(int index);
    
    @Column(name = SUBJECT)
    String getSubject();
    void setSubject(String subject);
  
    @Column(name = TYPE)
    int getType();
    void setType(int type);
    
    @Column(name = IS_DEFAULT)
    byte getIsDefault();
    void setIsDefault(byte isDefault);
    
    @Column(name = PERMISSION)
    int getPermission();
    void setPermission(int permission);
  }
  
  @Override
  public List<Ace> getAcesByPKBatched(long inodeId, int[] ids) throws StorageException {
    HopsSession session = connector.obtainSession();
    
    boolean activeConnector = session.currentTransaction().isActive();
    if(!activeConnector){
      session.currentTransaction().begin();
    }
  
    List<Ace> aces;
    List<AceDto> dtos = new ArrayList<>();
    try {
      for (int i = 0; i < ids.length; i++) {
        AceDto dto = session.newInstance(AceDto.class);
        dto.setInodeId(inodeId);
        dto.setIndex(ids[i]);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      aces = fromDtos(dtos);
    } finally {
      session.release(dtos);
    }
    
    if(!activeConnector){
     session.currentTransaction().commit();
    }
    return aces;
  }
  
  @Override
  public void prepare(Collection<Ace> removed, Collection<Ace> newed, Collection<Ace> modified) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    List<AceDto> deletions = new ArrayList<>();
    List<AceDto> additions = new ArrayList<>();
    List<AceDto> changes = new ArrayList<>();
    try {
      for (Ace ace : removed) {
        Object[] pk = new Object[2];
        pk[0] = ace.getInodeId();
        pk[1] = ace.getIndex();
        AceDto persistable = session.newInstance(AceDto.class, pk);
        deletions.add(persistable);
      }
      
      for (Ace ace : newed) {
        AceDto toAdd = createPersistable(session, ace);
        additions.add(toAdd);
      }
      
      for (Ace ace : modified) {
        AceDto persistable = createPersistable(session, ace);
        changes.add(persistable);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(additions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(additions);
      session.release(changes);
    }
  }
  
  private List<Ace> fromDtos(List<AceDto> dtos){
    ArrayList<Ace> toReturn = new ArrayList<>();
    for (AceDto dto : dtos) {
      toReturn.add(fromDto(dto));
    }
    return toReturn;
  }
  
  private Ace fromDto(AceDto dto){
    long inode = dto.getInodeId();
    int index = dto.getIndex();
    String subject = dto.getSubject();
    boolean isDefault = NdbBoolean.convert(dto.getIsDefault());
    int permission = dto.getPermission();
    Ace.AceType type = Ace.AceType.valueOf(dto.getType());
    
    return new Ace(inode, index, subject, type, isDefault, permission);
  }
  
  private AceDto createPersistable(HopsSession session, Ace from) throws StorageException {
    AceDto aceDto = session.newInstance(AceDto.class);
    aceDto.setInodeId(from.getInodeId());
    aceDto.setIndex(from.getIndex());
    aceDto.setSubject(from.getSubject());
    aceDto.setIsDefault(NdbBoolean.convert(from.isDefault()));
    aceDto.setPermission(from.getPermission());
    aceDto.setType(from.getType().getValue());
    return aceDto;
  }
}
