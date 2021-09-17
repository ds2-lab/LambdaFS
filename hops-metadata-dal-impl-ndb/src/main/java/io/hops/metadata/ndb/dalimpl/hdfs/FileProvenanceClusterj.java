/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2019  hops.io
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
import io.hops.metadata.hdfs.entity.FileProvenanceEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import io.hops.metadata.hdfs.dal.FileProvenanceDataAccess;

public class FileProvenanceClusterj implements TablesDef.FileProvenanceTableDef,
  FileProvenanceDataAccess<FileProvenanceEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface ProvenanceLogEntryDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);
    
    @PrimaryKey
    @Column(name = OPERATION)
    String getOperation();

    void setOperation(String operation);
    
    @PrimaryKey
    @Column(name = LOGICAL_TIME)
    int getLogicalTime();

    void setLogicalTime(int logicalTime);
    
    @PrimaryKey
    @Column(name = TIMESTAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);
    
    @PrimaryKey
    @Column(name = APP_ID)
    String getAppId();

    void setAppId(String appId);
    
    @PrimaryKey
    @Column(name = USER_ID)
    int getUserId();

    void setUserId(int userId);
  
    //This field is used in case two namenodes/threads, happen to perform a read operation
    //on the same inode, at the same time(logical and timestamp) on behalf of same user(and app).
    //We want to be able to store both occurrences,
    //since the parent dataset inode logical time might change(change of provenance xattr)
    //This is an unique id field (namenodeId_threadId) in the context of the conflicts stated above (tie breaker)
    //and is used by the processor of the file prov logs(ePipe)
    @PrimaryKey
    @Column(name = TIE_BREAKER)
    String getTieBreaker();
  
    void setTieBreaker(String tieBreaker);
    
    @Column(name = PARTITION_ID)
    long getPartitionId();

    void setPartitionId(long partitionId);
    
    @Column(name = PROJECT_ID)
    long getProjectId();

    void setProjectId(long projectId);
    
    @Column(name = DATASET_ID)
    long getDatasetId();

    void setDatasetId(long datasetId);
    
    @Column(name = PARENT_ID)
    long getParentId();

    void setParentId(long parentId);
    
    @Column(name = INODE_NAME)
    String getInodeName();

    void setInodeName(String inodeName);
    
    @Column(name = PROJECT_NAME)
    String getProjectName();

    void setProjectName(String projectName);
    
    @Column(name = DATASET_NAME)
    String getDatasetName();

    void setDatasetName(String datasetName);
    
    @Column(name = P1_NAME)
    String getP1Name();

    void setP1Name(String p1Name);
    
    @Column(name = P2_NAME)
    String getP2Name();

    void setP2Name(String p2Name);
    
    @Column(name = PARENT_NAME)
    String getParentName();

    void setParentName(String parentName);
    
    @Column(name = USER_NAME)
    String getUserName();

    void setUserName(String userName);
    
    @Column(name = XATTR_NAME)
    String getXAttrName();

    void setXAttrName(String xattrName);
    
    @Column(name = LOGICAL_TIME_BATCH)
    int getLogicalTimeBatch();

    void setLogicalTimeBatch(int logicalTimeBatch);
    
    @Column(name = TIMESTAMP_BATCH)
    long getTimestampBatch();

    void setTimestampBatch(long timestampBatch);
  
    @Column(name = DS_LOGICAL_TIME)
    int getDsLogicalTime();
  
    void setDsLogicalTime(int dsLogicalTime);
  
    @Column(name = XATTR_NUM_PARTS)
    short getXAttrNumParts();
  
    void setXAttrNumParts(short numParts);
  }

  @Override
  public void addAll(Collection<FileProvenanceEntry> logEntries)
    throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<ProvenanceLogEntryDto> added = new ArrayList<>(logEntries.size());
    try {
      for (FileProvenanceEntry logEntry : logEntries) {
        added.add(createPersistable(logEntry));
      }
      session.savePersistentAll(added);
    } finally {
      session.release(added);
    }
  }

  @Override
  public void add(FileProvenanceEntry logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    ProvenanceLogEntryDto dto = null;
    try {
      dto = createPersistable(logEntry);
      session.savePersistent(dto);
    } finally {
      session.release(dto);
    }
  }

  private ProvenanceLogEntryDto createPersistable(FileProvenanceEntry logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    ProvenanceLogEntryDto dto = session.newInstance(ProvenanceLogEntryDto.class);
    dto.setInodeId(logEntry.getInodeId());
    dto.setOperation(logEntry.getOperation());
    dto.setLogicalTime(logEntry.getLogicalTime());
    dto.setTimestamp(logEntry.getTimestamp());
    dto.setAppId(logEntry.getAppId());
    dto.setUserId(logEntry.getUserId());
    dto.setTieBreaker(logEntry.getTieBreaker());
    dto.setPartitionId(logEntry.getPartitionId());
    dto.setProjectId(logEntry.getProjectId());
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setParentId(logEntry.getParentId());
    dto.setInodeName(logEntry.getInodeName());
    dto.setProjectName(logEntry.getProjectName());
    dto.setDatasetName(logEntry.getDatasetName());
    dto.setP1Name(logEntry.getP1Name());
    dto.setP2Name(logEntry.getP2Name());
    dto.setParentName(logEntry.getParentName());
    dto.setUserName(logEntry.getUserName());
    dto.setXAttrName(logEntry.getXattrName());
    dto.setLogicalTimeBatch(logEntry.getLogicalTimeBatch());
    dto.setTimestampBatch(logEntry.getTimestampBatch());
    dto.setDsLogicalTime(logEntry.getDsLogicalTime());
    dto.setXAttrNumParts(logEntry.getXattrNumParts());
    return dto;
  }
}
