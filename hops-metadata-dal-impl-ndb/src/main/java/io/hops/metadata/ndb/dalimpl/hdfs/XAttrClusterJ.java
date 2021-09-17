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
import com.google.common.primitives.Bytes;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XAttrClusterJ implements TablesDef.XAttrTableDef,
    XAttrDataAccess<StoredXAttr, StoredXAttr.PrimaryKey> {
  
  static final Logger LOG = Logger.getLogger(XAttrClusterJ.class);
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface XAttrDTO {
    
    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    
    void setINodeId(long inodeId);
  
    @PrimaryKey
    @Column(name = NAMESPACE)
    byte getNamespace();
  
    void setNamespace(byte id);
  
    @PrimaryKey
    @Column(name = NAME)
    String getName();
    
    void setName(String name);
  
    @PrimaryKey
    @Column(name = INDEX)
    short getIndex();
  
    void setIndex(short index);
  
    @Column(name = NUM_PARTS)
    short getNumParts();
  
    void setNumParts(short numParts);
    
    @Column(name = VALUE)
    byte[] getValue();
  
    void setValue(byte[] value);
  }
  
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private short NON_EXISTS_XATTR = -1;
  
  @Override
  public List<StoredXAttr> getXAttrsByPrimaryKeyBatch(
      List<StoredXAttr.PrimaryKey> pks) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<XAttrDTO> dtos = Lists.newArrayListWithExpectedSize(pks.size());
    List<List<XAttrDTO>> partsDtos = Lists.newArrayListWithCapacity(pks.size());
    try {
      
      short index = 0;
      for (StoredXAttr.PrimaryKey pk : pks) {
        XAttrDTO dto = session.newInstance(XAttrDTO.class,
            new Object[]{pk.getInodeId(), pk.getNamespace(), pk.getName(), index});
        dto.setNumParts(NON_EXISTS_XATTR);
        session.load(dto);
        dtos.add(dto);
      }
      session.flush();
  
      
      for(XAttrDTO dto : dtos){
        //check if the row exists
        if(dto.getNumParts() != NON_EXISTS_XATTR) {
          List<XAttrDTO> pdtos =
              Lists.newArrayListWithExpectedSize(dto.getNumParts());
          pdtos.add(dto);
          for(short i=1; i<dto.getNumParts(); i++){
            XAttrDTO partDto = session.newInstance(XAttrDTO.class,
                new Object[]{dto.getINodeId(), dto.getNamespace(),
                    dto.getName(), i});
            partDto.setNumParts(NON_EXISTS_XATTR);
            session.load(partDto);
            pdtos.add(partDto);
          }
          partsDtos.add(pdtos);
        }
      }
      
      session.flush();
      
      return convertBatch(session, partsDtos);
    }finally {
      session.release(dtos);
      for(List<XAttrDTO> dtoList : partsDtos){
        session.release(dtoList);
      }
    }
  }
  
  @Override
  public Collection<StoredXAttr> getXAttrsByInodeId(long inodeId) throws StorageException{
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<XAttrDTO> dobj =
        qb.createQueryDefinition(XAttrDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("idParam"));
    dobj.where(pred1);
  
    HopsQuery<XAttrDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);
  
    List<XAttrDTO> results = null;
    try {
      results = query.getResultList();
      if (results.isEmpty()) {
        return null;
      }
      return convertByInode(session, results);
    }finally {
      session.release(results);
    }
  
  }
  
  @Override
  public int removeXAttrsByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<XAttrDTO> dobj =
        qb.createQueryDefinition(XAttrDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("idParam"));
    dobj.where(pred1);
  
    HopsQuery<XAttrDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);
    return query.deletePersistentAll();
  }
  
  @Override
  public void prepare(Collection<StoredXAttr> removed, Collection<StoredXAttr> newed,
      Collection<StoredXAttr> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<XAttrDTO> changes = new ArrayList<>();
    List<XAttrDTO> deletions = new ArrayList<>();
    try {
      for (StoredXAttr xattr : removed) {
        List<XAttrDTO> persistables = createPersistable(session, xattr);
        deletions.addAll(persistables);
      }
    
      for (StoredXAttr xattr : newed) {
        List<XAttrDTO> persistables = createPersistable(session, xattr);
        List<XAttrDTO> xdeletions = createExtraDeletionPersistables(session,
            xattr);
        deletions.addAll(xdeletions);
        changes.addAll(persistables);
      }
    
      for (StoredXAttr xattr : modified) {
        List<XAttrDTO> persistables = createPersistable(session, xattr);
        List<XAttrDTO> xdeletions = createExtraDeletionPersistables(session,
            xattr);
        deletions.addAll(xdeletions);
        changes.addAll(persistables);
      }
      
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }
  
  @Override
  public int count() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }
  
  private List<XAttrDTO> createPersistable(HopsSession session,
      StoredXAttr xattr) throws StorageException {
    List<XAttrDTO> xAttrDTOS = new ArrayList<>();
    short numParts = xattr.getNumParts();
    for(short index=0; index < numParts; index++){
      XAttrDTO dto = session.newInstance(XAttrDTO.class);
      dto.setINodeId(xattr.getInodeId());
      dto.setNamespace(xattr.getNamespace());
      dto.setName(xattr.getName());
      dto.setValue(xattr.getValue(index));
      dto.setIndex(index);
      dto.setNumParts(numParts);
      xAttrDTOS.add(dto);
    }
    return xAttrDTOS;
  }
  
  private List<XAttrDTO> createExtraDeletionPersistables(HopsSession session,
      StoredXAttr xattr) throws StorageException {
    List<XAttrDTO> xAttrDTOS = new ArrayList<>();
    if(xattr.getOldNumParts() > 0 && xattr.getNumParts() < xattr.getOldNumParts()){
      for(short index =xattr.getNumParts(); index<xattr.getOldNumParts(); index++){
        XAttrDTO dto = session.newInstance(XAttrDTO.class);
        dto.setINodeId(xattr.getInodeId());
        dto.setNamespace(xattr.getNamespace());
        dto.setName(xattr.getName());
        dto.setIndex(index);
        xAttrDTOS.add(dto);
      }
    }
    return xAttrDTOS;
  }
  
  private List<StoredXAttr> convertBatch(HopsSession session,
      List<List<XAttrDTO>> dtos) throws StorageException {
    List<StoredXAttr> results = Lists.newArrayListWithExpectedSize(dtos.size());
    for(List<XAttrDTO> dtoList : dtos){
      results.add(convert(session, dtoList));
    }
    return results;
  }
  
  private List<StoredXAttr> convertByInode(HopsSession session,
      List<XAttrDTO> dtos) throws StorageException {
    List<StoredXAttr> results = Lists.newArrayList();
    Map<StoredXAttr.PrimaryKey, List<XAttrDTO>> xAttrsByPk = new HashMap<>();
    for(XAttrDTO dto : dtos){
      StoredXAttr.PrimaryKey pk = new StoredXAttr.PrimaryKey(dto.getINodeId()
          , dto.getNamespace(), dto.getName());
      if(!xAttrsByPk.containsKey(pk)){
        xAttrsByPk.put(pk, new ArrayList<XAttrDTO>());
      }
      xAttrsByPk.get(pk).add(dto);
    }
    
    for(Map.Entry<StoredXAttr.PrimaryKey, List<XAttrDTO>> entry :
        xAttrsByPk.entrySet()){
      List<XAttrDTO> xAttrDTOS = entry.getValue();
      Collections.sort(xAttrDTOS, new Comparator<XAttrDTO>() {
        @Override
        public int compare(XAttrDTO o1, XAttrDTO o2) {
          return Short.compare(o1.getIndex(), o2.getIndex());
        }
      });
      
      results.add(convert(session, xAttrDTOS));
    }
    return results;
  }
  
  private StoredXAttr convert(HopsSession session, List<XAttrDTO> dtos)
      throws StorageException {
    byte[][] values = new byte[dtos.size()][];
    short index = 0;
    int nulls = 0;
    for(XAttrDTO dto : dtos){
      if(dto.getNumParts() != NON_EXISTS_XATTR){
        values[index] = dto.getValue();
      }else{
        XAttrDTO partDto = session.find(XAttrDTO.class,
            new Object[]{dto.getINodeId(), dto.getNamespace(),
                dto.getName(), index});
        values[index] = partDto.getValue();
      }
      if(values[index] == null){
        nulls++;
      }
      index++;
    }
  
    XAttrDTO dto = dtos.get(0);
    
    byte[] value;
    if(nulls == 0){
      value = Bytes.concat(values);
    }else if(nulls == dtos.size()){
      value = null;
    }else{
      throw new IllegalStateException("Failed to read XAttr [ " + dto.getName()
          +  " ] for Inode " + dto.getINodeId() + " because " + nulls +
          " parts were null.");
    }
    
    return new StoredXAttr(dto.getINodeId(), dto.getNamespace(),
        dto.getName(), value);
  }
  
}
