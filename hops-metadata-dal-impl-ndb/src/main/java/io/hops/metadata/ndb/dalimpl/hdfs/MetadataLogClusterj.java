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
import io.hops.exception.UnknownMetadataOperationType;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;

public class MetadataLogClusterj implements TablesDef.MetadataLogTableDef,
    MetadataLogDataAccess<MetadataLogEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface MetadataLogEntryDto {
    @PrimaryKey
    @Column(name = DATASET_ID)
    long getDatasetId();

    void setDatasetId(long datasetId);

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @PrimaryKey
    @Column(name = Logical_TIME)
    int getLogicalTime();

    void setLogicalTime(int logicalTime);

    @Column(name = PK1)
    long getPk1();

    void setPk1(long pk1);

    @Column(name = PK2)
    long getPk2();

    void setPk2(long pk2);

    @Column(name = PK3)
    String getPk3();

    void setPk3(String pk3);

    @Column(name = OPERATION)
    short getOperation();
  
    @Column(name = INODE_PARTITION_ID)
    long getInodePartitionId();
  
    void setInodePartitionId(long partitionId);
  
    @Column(name = INODE_PARENT_ID)
    long getInodeParentId();
  
    void setInodeParentId(long parentId);
  
    @Column(name = INODE_NAME)
    String getInodeName();
  
    void setInodeName(String name);

    void setOperation(short operation);
  }


  @PersistenceCapable(table = LOOKUP_TABLE_NAME)
  public interface DatasetINodeLookupDTO{

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = DATASET_ID)
    long getDatasetId();

    void setDatasetId(long datasetId);
  }

  @Override
  public void addAll(Collection<MetadataLogEntry> logEntries)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<MetadataLogEntryDto> added = new ArrayList<>(
        logEntries.size());
    ArrayList<DatasetINodeLookupDTO> newLookupDTOS = new
        ArrayList<>(logEntries.size());
    try {
      for (MetadataLogEntry logEntry : logEntries) {
        added.add(createPersistable(logEntry));
        
        if(INodeMetadataLogEntry.isValidOperation(logEntry.getOperationId())) {
          INodeMetadataLogEntry iNodeLogEntry =
              (INodeMetadataLogEntry) logEntry;
          DatasetINodeLookupDTO lookupDTO = createLookupPersistable(logEntry);
          if (iNodeLogEntry.getOperation() == INodeMetadataLogEntry.Operation.Add) {
            newLookupDTOS.add(lookupDTO);
          } else if (iNodeLogEntry.getOperation() ==
              INodeMetadataLogEntry.Operation.Delete) {
            session.deletePersistent(lookupDTO);
            session.release(lookupDTO);
          }
        }
      }

      session.makePersistentAll(added);
      session.savePersistentAll(newLookupDTOS);
    }finally {
      session.release(added);
      session.release(newLookupDTOS);
    }
  }

  @Override
  public void add(MetadataLogEntry metadataLogEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = null;
    DatasetINodeLookupDTO lookupDTO = null;
    try {
      dto = createPersistable(metadataLogEntry);
      session.makePersistent(dto);
      
      if(INodeMetadataLogEntry.isValidOperation(metadataLogEntry.getOperationId())) {
        lookupDTO = createLookupPersistable(metadataLogEntry);
        
        INodeMetadataLogEntry iNodeMetadataLogEntry =
            (INodeMetadataLogEntry) metadataLogEntry;
        if (iNodeMetadataLogEntry.getOperation() == INodeMetadataLogEntry.Operation.Add) {
          session.savePersistent(lookupDTO);
        } else if (iNodeMetadataLogEntry.getOperation() ==
            INodeMetadataLogEntry.Operation.Delete) {
          session.deletePersistent(lookupDTO);
        }
      }
    }finally {
      session.release(dto);
      session.release(lookupDTO);
    }
  }

  private MetadataLogEntryDto createPersistable(MetadataLogEntry logEntry)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = session.newInstance(MetadataLogEntryDto.class);
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setInodeId(logEntry.getInodeId());
    dto.setPk1(logEntry.getPk1());
    dto.setPk2(logEntry.getPk2());
    dto.setPk3(logEntry.getPk3());
    dto.setLogicalTime(logEntry.getLogicalTime());
    dto.setOperation(logEntry.getOperationId());
    dto.setInodePartitionId(logEntry.getInodePartitionId());
    dto.setInodeParentId(logEntry.getInodeParentId());
    dto.setInodeName(logEntry.getInodeName());
    return dto;
  }

  private DatasetINodeLookupDTO createLookupPersistable(MetadataLogEntry
      logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    DatasetINodeLookupDTO dto = session.newInstance(DatasetINodeLookupDTO
        .class);
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setInodeId(logEntry.getInodeId());
    return dto;
  }

  @Override
  public Collection<MetadataLogEntry> find(long fileId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<MetadataLogEntryDto> dobj =
        qb.createQueryDefinition(MetadataLogEntryDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeIdParam"));
    dobj.where(pred1);
    HopsQuery<MetadataLogEntryDto> query = session.createQuery(dobj);
    query.setParameter("inodeIdParam", fileId);
    
    Collection<MetadataLogEntryDto> dtos = query.getResultList();
    Collection<MetadataLogEntry> mlel = createCollection(dtos);
    session.release(dtos);
    return mlel;
  }

  private Collection<MetadataLogEntry> createCollection(
      Collection<MetadataLogEntryDto> collection)
      throws UnknownMetadataOperationType {
    ArrayList<MetadataLogEntry> list =
        new ArrayList<>(collection.size());
    for (MetadataLogEntryDto dto : collection) {
      list.add(createMetadataLogEntry(dto));
    }
    return list;
  }

  private MetadataLogEntry createMetadataLogEntry(MetadataLogEntryDto dto)
      throws UnknownMetadataOperationType {
    return MetadataLogEntry.newEntry(
        dto.getDatasetId(),
        dto.getInodeId(),
        dto.getLogicalTime(),
        dto.getInodePartitionId(),
        dto.getInodeParentId(),
        dto.getInodeName(),
        dto.getPk1(),
        dto.getPk2(),
        dto.getPk3(),
        dto.getOperation());
  }
}
