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

public class INodeIdentifier implements Comparable<INodeIdentifier>{

  Long inodeID;
  Long pid;
  String  name;
  Long partitionId;
  Short   depth;
  byte storagePolicy;

  public INodeIdentifier() {
    this(-1L, null, null, null);
  }
  
  public INodeIdentifier(Long inodeID) {
    this(inodeID, null, null, null);
  }

  public INodeIdentifier(Long inodeID, Long parentId, String name, Long partitionId) {
    this.inodeID = inodeID;
    this.pid = parentId;
    this.name = name;
    this.partitionId = partitionId;
    this.depth = null;
  }

  public Short getDepth() {
    return depth;
  }

  public void setDepth(Short depth) {
    this.depth = depth;
  }

  public Long getInodeId() {
    return inodeID;
  }

  public Long getPid() {
    return pid;
  }

  public String getName() {
    return name;
  }

  public void setPid(Long pid) {
    this.pid = pid;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setPartitionId(Long partitionId){
    this.partitionId = partitionId;
  }

  public Long getPartitionId(){
    return partitionId;
  }

    public byte getStoragePolicy() {
        return storagePolicy;
    }

    public void setStoragePolicy(byte storagePolicy) {
        this.storagePolicy = storagePolicy;
    }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 47 * hash + (this.inodeID != null ? this.inodeID.hashCode() : 0);
    hash = 47 * hash + (this.pid != null ? this.pid.hashCode() : 0);
    hash = 47 * hash + (this.name != null ? this.name.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final INodeIdentifier other = (INodeIdentifier) obj;
    if (this.inodeID != other.inodeID &&
        (this.inodeID == null || !this.inodeID.equals(other.inodeID))) {
      return false;
    }
    if (this.pid != other.pid &&
        (this.pid == null || !this.pid.equals(other.pid))) {
      return false;
    }
    if ((this.name == null) ? (other.name != null) :
        !this.name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "INodeIdentifier{" + "inodeID=" + inodeID + ", pid=" + pid +
        ", name=" + name + '}';
  }
  
  @Override
  public int compareTo(INodeIdentifier other){
    return this.inodeID.compareTo(other.inodeID);
  }
}
