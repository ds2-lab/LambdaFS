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

import com.google.common.primitives.SignedBytes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class INode extends INodeBase implements Comparable<INode> {

  private long modificationTime;
  private long accessTime;
  private String clientName;
  private String clientMachine;
  private int generationStamp;
  private String symlink;
  private MetaStatus metaStatus = MetaStatus.DISABLED;
  private boolean isFileStoredInDB;
  private int childrenNum = 0;
  
  public INode() {
//    this.modificationTime = -1;
//    this.accessTime = -1;
//    this.clientName = "";
//    this.clientMachine = "";
//    this.clientNode = "";
//    this.generationStamp = -1;
//    this.symlink = "";
//    this.metaStatus = false;
//    this.isFileStoredInDB = false;
  }

  public INode(long id, String name, long parentId, long partitionId, boolean isDir, boolean dirWithQuota,
      long modificationTime, long accessTime, int userID, int
      groupID, short permission, boolean underConstruction, String clientName,
      String clientMachine,
      int generationStamp, long header, String symlink,
      boolean subtreeLocked, long subtreeLockOwner, byte metaStatus,
      long size, boolean isFileStoredInDB, int logicalTime,
      byte storagePolicy, int childrenNum, int numAces, byte numUserXAttrs,
      byte numSysXAttrs) {

    super(id, parentId, name, partitionId, isDir, userID, groupID, permission, header,
        dirWithQuota, underConstruction, subtreeLocked, subtreeLockOwner,
        size, logicalTime, storagePolicy, numAces, numUserXAttrs, numSysXAttrs);

    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.generationStamp = generationStamp;
    this.symlink = symlink;
    this.metaStatus = MetaStatus.fromVal(metaStatus);
    this.isFileStoredInDB = isFileStoredInDB;
    this.childrenNum = childrenNum;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  public String getClientName() {
    return clientName;
  }

  public void setClientName(String clientName) {
    this.clientName = clientName;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public void setClientMachine(String clientMachine) {
    this.clientMachine = clientMachine;
  }

  public int getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStamp(int generationStamp) {
    this.generationStamp = generationStamp;
  }

  public String getSymlink() {
    return symlink;
  }

  public void setSymlink(String symlink) {
    this.symlink = symlink;
  }

  public MetaStatus getMetaStatus() {
    return metaStatus;
  }

  public void setMetaStatus(MetaStatus metaStatus) {
    this.metaStatus = metaStatus;
  }

  public boolean isFileStoredInDB(){ return isFileStoredInDB; }

  public void setFileStoredInDB(boolean isFileStoredInDB){ this.isFileStoredInDB = isFileStoredInDB; }
  
  @Override
  public final int compareTo(INode other) {
    String left = name == null ? "" : name;
    String right = other.name == null ? "" : other.name;
    return SignedBytes.lexicographicalComparator()
        .compare(left.getBytes(), right.getBytes());
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof INode)) {
      return false;
    }
    if (name.equals(((INode) that).name) && this.id == ((INode) that).id &&
        this.parentId == ((INode) that).parentId) {
      return true;
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return Arrays.hashCode(this.name.getBytes());
  }

  public static enum Order implements Comparator<INode> {

    ByName() {
      @Override
      public int compare(INode o1, INode o2) {
        return o1.compareTo(o2);
      }
    };

    @Override
    public abstract int compare(INode o1, INode o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }
  
  public int getChildrenNum() {
    return childrenNum;
  }

  public void setChildrenNum(int childrenNum) {
    this.childrenNum = childrenNum;
  }
}
