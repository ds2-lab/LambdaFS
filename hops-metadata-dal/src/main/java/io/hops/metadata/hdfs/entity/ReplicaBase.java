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

import java.util.Objects;

public abstract class ReplicaBase implements Comparable<ReplicaBase> {

  protected int storageId;
  protected long blockId;
  protected long inodeId;

  public ReplicaBase(int storageId, long blockId, long inodeId) {
    this.storageId = storageId;
    this.blockId = blockId;
    this.inodeId = inodeId;
  }

  /**
   * @return the storageId
   */
  public int getStorageId() {
    return storageId;
  }

  /**
   * @param storageId
   *     the storageId to set
   */
  public void setStorageId(int storageId) {
    this.storageId = storageId;
  }

  /**
   * @return the blockId
   */
  public long getBlockId() {
    return blockId;
  }

  /**
   * @param blockId
   *     the blockId to set
   */
  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(int inodeId) {
    this.inodeId = inodeId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageId,inodeId,blockId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    Replica replica = (Replica) o;

    return (storageId == replica.storageId &&
            blockId == replica.blockId);
  }

  @Override
  public int compareTo(ReplicaBase t) {
    if (t == null) {
      return 1;
    }

    int compVal = new Integer(storageId).compareTo(t.storageId);
    if(compVal != 0){
      return  compVal;
    }

    compVal = new Long(inodeId).compareTo(t.inodeId);
    if(compVal != 0){
      return  compVal;
    }

    compVal = new Long(blockId).compareTo(t.blockId);
    if(compVal != 0){
      return  compVal;
    }

    return 0;
  }

  @Override
  public String toString() {
    return "ReplicaBase{" +
        "storageId=" + storageId +
        ", blockId=" + blockId +
        ", inodeId=" + inodeId +
        '}';
  }
}
