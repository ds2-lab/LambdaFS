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
package io.hops.metadata.hdfs.entity;

import io.hops.exception.UnknownMetadataOperationType;

public class MetadataLogEntry {
  
  public interface OperationBase{
    short getId();
  }
  
  private final long datasetId;
  private final long inodeId;
  private final long pk1;
  private final long pk2;
  private final String pk3;
  private int logicalTime;
  private final short operation;
  private final long inodePartitionId;
  private final long inodeParentId;
  private final String inodeName;
  
  protected MetadataLogEntry(MetadataLogEntry entry){
    this(entry.getDatasetId(), entry.getInodeId(), entry.getLogicalTime(),
        entry.getInodePartitionId(), entry.getInodeParentId(),
        entry.getInodeName(), entry.getPk1(), entry.getPk2(), entry.getPk3(),
        entry.getOperationId());
  }
  
  protected MetadataLogEntry(long datasetId, long inodeId, int logicalTime,
      long inodePartitionId, long inodeParentId, String inodeName, short operation) {
    this(datasetId, inodeId, logicalTime, inodePartitionId, inodeParentId,
        inodeName, -1, -1, "-1", operation);
  }
  
  protected MetadataLogEntry(long datasetId, long inodeId, int logicalTime,
      long inodePartitionId, long inodeParentId, String inodeName,
      long pk1, long pk2, String pk3, short operation) {
    this.datasetId = datasetId;
    this.inodeId = inodeId;
    this.logicalTime = logicalTime;
    
    this.inodePartitionId = inodePartitionId;
    this.inodeParentId = inodeParentId;
    this.inodeName = inodeName;
    
    this.pk1 = pk1;
    this.pk2 = pk2;
    this.pk3 = pk3;
    
    this.operation = operation;
  }
  
  public long getDatasetId() {
    return datasetId;
  }

  public long getInodeId() {
    return inodeId;
  }

  public int getLogicalTime() {
    return logicalTime;
  }
  
  public short getOperationId(){
    return operation;
  }
  
  public long getPk1() {
    return pk1;
  }
  
  public long getPk2() {
    return pk2;
  }
  
  public String getPk3() {
    return pk3;
  }
  
  public long getInodePartitionId() {
    return inodePartitionId;
  }
  
  public long getInodeParentId() {
    return inodeParentId;
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public static MetadataLogEntry newEntry(long datasetId, long inodeId,
      int logicalTime, long inodePartitionId, long inodeParentId,
      String inodeName, long pk1, long pk2, String pk3, short operationId)
      throws UnknownMetadataOperationType {
    if(INodeMetadataLogEntry.isValidOperation(operationId)){
      return new INodeMetadataLogEntry(new MetadataLogEntry(datasetId,
          inodeId, logicalTime, inodePartitionId, inodeParentId, inodeName,
          pk1, pk2, pk3, operationId));
    }
    
    if(XAttrMetadataLogEntry.isValidOperation(operationId)){
      return new XAttrMetadataLogEntry(new MetadataLogEntry(datasetId,
          inodeId, logicalTime, inodePartitionId, inodeParentId, inodeName,
          pk1, pk2, pk3, operationId));
    }
    
    throw new UnknownMetadataOperationType(operationId);
  }
  
  @Override
  public String toString() {
    return "MetadataLogEntry{" +
        "datasetId=" + datasetId +
        ", inodeId=" + inodeId +
        ", pk1=" + pk1 +
        ", pk2=" + pk2 +
        ", pk3='" + pk3 + '\'' +
        ", logicalTime=" + logicalTime +
        ", operation=" + operation +
        '}';
  }
}
