/*
 * Copyright (C) 2019 hops.io.
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
package io.hops.metadata.hdfs.entity;

public class FileProvenanceEntry {

  private final long inodeId;
  private final Operation operation;
  private final int logicalTime;
  private final long timestamp;
  private final String appId;
  private final int userId;
  private final String tieBreaker;
  private final long partitionId;
  private final long projectId;
  private final long datasetId;
  private final long parentId;
  private final String inodeName;
  private final String projectName;
  private final String datasetName;
  private final String p1Name;
  private final String p2Name;
  private final String parentName;
  private final String userName;
  private final String xattrName;
  private final int logicalTimeBatch;
  private final long timestampBatch;
  private final int dsLogicalTime;
  private final short xattrNumParts;
  
  public static enum Operation {
    CREATE,
    DELETE,
    ACCESS_DATA,
    MODIFY_DATA,
    METADATA,
    XATTR_ADD,
    XATTR_UPDATE,
    XATTR_DELETE,
    OTHER;

    public static FileProvenanceEntry.Operation create() {
      return Operation.CREATE;
    }
    
    public static FileProvenanceEntry.Operation delete() {
      return Operation.DELETE;
    }
    
    public static FileProvenanceEntry.Operation getBlockLocations() {
      return Operation.ACCESS_DATA;
    }
    
    public static FileProvenanceEntry.Operation append() {
      return Operation.MODIFY_DATA;
    }
    
    public static FileProvenanceEntry.Operation concat() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation createSymlink() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation setPermission() {
      return Operation.METADATA;
    }

    public static FileProvenanceEntry.Operation setOwner() {
      return Operation.METADATA;
    }
    
    public static FileProvenanceEntry.Operation setTimes() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation setReplication() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation setMetaEnabled() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation setStoragePolicy() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation getfileinfo() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation listStatus() {
      return Operation.OTHER;
    }
    
    public static FileProvenanceEntry.Operation addXAttr() {
      return Operation.XATTR_ADD;
    }
  }

  public FileProvenanceEntry(long inodeId, Operation operation, int logicalTime, long timestamp,
    String appId, int userId, String tieBreaker,
    long partitionId, long projectId, long datasetId, long parentId, 
    String inodeName, String projectName, String datasetName, String p1Name, String p2Name, String parentName,
    String userName, String xattrName, int logicalTimeBatch,
      long timestampBatch, int dsLogicalTime, byte[] xAttrValue) {
    this.inodeId = inodeId;
    this.operation = operation;
    this.logicalTime = logicalTime;
    this.timestamp = timestamp;
    this.appId = appId;
    this.userId = userId;
    this.tieBreaker = tieBreaker;
    this.partitionId = partitionId;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.parentId = parentId;
    this.inodeName = inodeName;
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.p1Name = p1Name;
    this.p2Name = p2Name;
    this.parentName = parentName;
    this.userName = userName;
    this.xattrName = xattrName;
    this.logicalTimeBatch = logicalTimeBatch;
    this.timestampBatch = timestampBatch;
    this.dsLogicalTime = dsLogicalTime;
    this.xattrNumParts = StoredXAttr.getNumParts(xAttrValue);
  }

  public long getInodeId() {
    return inodeId;
  }

  public int getUserId() {
    return userId;
  }

  public String getAppId() {
    return appId;
  }
  
  public String getTieBreaker() {
    return tieBreaker;
  }
  
  public int getLogicalTime() {
    return logicalTime;
  }

  public int getLogicalTimeBatch() {
    return logicalTimeBatch;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getTimestampBatch() {
    return timestampBatch;
  }

  public long getPartitionId() {
    return partitionId;
  }

  public long getProjectId() {
    return projectId;
  }

  public long getDatasetId() {
    return datasetId;
  }
  
  public long getParentId() {
    return parentId;
  }
  
  public String getInodeName() {
    return inodeName;
  }

  public Operation getOperationEnumVal() {
    return operation;
  }

  public String getOperation() {
    return operation.toString();
  }

  public String getP1Name() {
    return p1Name;
  }

  public String getP2Name() {
    return p2Name;
  }

  public String getParentName() {
    return parentName;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public String getProjectName() {
    return projectName;
  }

  public String getUserName() {
    return userName;
  }
  
  public String getXattrName() {
    return xattrName;
  }
  
  public int getDsLogicalTime() {
    return dsLogicalTime;
  }
  
  public short getXattrNumParts() {
    return xattrNumParts;
  }
  
  @Override
  public String toString() {
    return "ProvenanceLogEntry{"
      + "inodeId=" + inodeId
      + ", operation=" + operation
      + ", logicalTime=" + logicalTime
      + ", timestamp=" + timestamp
      + ", appId=" + appId
      + ", userId=" + userId
      + ", tieBreaker=" + tieBreaker
      + ", partitionId=" + partitionId
      + ", projectId=" + projectId
      + ", datasetId=" + datasetId
      + ", parentId=" + parentId
      + ", inodeName=" + inodeName
      + ", projectName=" + projectName
      + ", datasetName=" + datasetName
      + ", p1Name=" + p1Name
      + ", p2Name=" + p2Name
      + ", parentName=" + parentName
      + ", userName=" + userName
      + ", xattrName=" + xattrName
      + ", logicalTimeBatch=" + logicalTimeBatch
      + ", timestampBatch=" + timestampBatch
      + ", dsLogicalTime=" + dsLogicalTime
      + '}';
  }
}
