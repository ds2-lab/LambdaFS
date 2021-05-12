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

import io.hops.metadata.common.FinderType;

public class SubTreeOperation implements Comparable<SubTreeOperation> {

  public static enum Type {
    RENAME_STO, //STO = Sub Tree Operation
    DELETE_STO,
    CONTENT_SUMMARY_STO,
    QUOTA_STO,
    SET_PERMISSION_STO,
    SET_OWNER_STO,
    META_ENABLE_STO,
    NA;
  }
  public static enum Finder implements FinderType<SubTreeOperation> {
    ByPathPrefix, ByPath;

    @Override
    public Class getType() {
      return SubTreeOperation.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByPath:
          return Annotation.PrimaryKey;
        case ByPathPrefix:
          return Annotation.IndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private long nameNodeId;
  private String path;
  private Type opType;
  private long asyncLockRecoveryTime = 0;  // contains the time when async was enabled. 0 = disabled
  private long startTime;
  private String user;
  private long inodeID;

  public SubTreeOperation(String path, long inodeID,  long nameNodeId, Type opType, long startTime,
                          String user) {
    this(path, inodeID, nameNodeId, opType, startTime, user, 0);
  }

  public SubTreeOperation(String path, long inodeID, long nameNodeId, Type opType,
                          long startTime, String user, long asyncLockRecovery) {
    this.path = path;
    this.nameNodeId = nameNodeId;
    this.opType = opType;
    this.startTime = startTime;
    this.user = user;
    this.asyncLockRecoveryTime = asyncLockRecovery;
    this.inodeID = inodeID;
  }

  /**
   * @return the name node id
   */
  public long getNameNodeId() {
    return nameNodeId;
  }

  /**
   * @param nameNodeId namenode id
   *     set the name node id
   */
  public void setHolderId(long nameNodeId) {
    this.nameNodeId = nameNodeId;
  }

  /**
   * @return the path
   */
  public String getPath() {
    return path;
  }

  /**
   * @param path
   *     the path to set
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * 
   * @return returns the operation type
   */
  public Type getOpType() {
    return opType;
  }

  /**
   * 
   * @param opType  set the operation type
   */
  public void setOpType(Type opType) {
    this.opType = opType;
  }

  /**
   * get async recovery start time. 0 = disabled
   */
  public long getAsyncLockRecoveryTime() {
    return asyncLockRecoveryTime;
  }

  /**
   * Enable async recovery
   * @param asyncLockRecoveryTime
   */
  public void setAsyncLockRecoveryTime(long asyncLockRecoveryTime) {
    this.asyncLockRecoveryTime = asyncLockRecoveryTime;
  }

  /**
   * Get start time of the sub tree operation
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set start time of the subtree operation
   * @param startTime start time
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get the user name that started the operation
   */
  public String getUser() {
    return user;
  }

  /**
   * Set the user name
   * @param user
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * Get inode ID of the subtree root
   */
  public long getInodeID() {
    return inodeID;
  }

  /**
   * Set inode ID of the subtree root
   * @param inodeID of subtree root
   */
  public void setInodeID(long inodeID) {
    this.inodeID = inodeID;
  }

  @Override
  public int compareTo(SubTreeOperation t) {
    return this.path.compareTo(t.getPath());
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof SubTreeOperation)){
      return false;
    }
    SubTreeOperation other = (SubTreeOperation) obj;
    return (this.path.equals(other.getPath()) 
            && this.nameNodeId == other.getNameNodeId() && 
            this.opType == other.opType
            );
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.path != null ? this.path.hashCode() : 0);
    hash = 37 * hash + (new Long(nameNodeId)).hashCode();
    hash = 37 * hash + opType.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return "Path: " + this.path + ", INodeID: "+ inodeID+", NN ID: " + nameNodeId
            + ", Operation: " + opType + ", User: " + user +
            ", Start Time:" + (new java.util.Date(startTime)).toString() +
            (
                    asyncLockRecoveryTime > 0 ?
                    ", Recovery Start Time " + (new java.util.Date(asyncLockRecoveryTime)).toString()
                    : ""
            );
  }
}
