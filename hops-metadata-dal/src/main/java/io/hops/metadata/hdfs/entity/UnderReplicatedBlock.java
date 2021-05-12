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

import java.util.Collections;
import java.util.Comparator;

public class UnderReplicatedBlock {
  public static enum Finder implements FinderType<UnderReplicatedBlock> {

    ByBlockIdAndINodeId,
    ByINodeId,
    ByINodeIds;

    @Override
    public Class getType() {
      return UnderReplicatedBlock.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndINodeId:
          return Annotation.PrimaryKey;
        case ByINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }
  
  public static enum Order implements Comparator<UnderReplicatedBlock> {

    ByLevel() {
      @Override
      public int compare(UnderReplicatedBlock o1, UnderReplicatedBlock o2) {
        if (o1.getLevel() < o2.level) {
          return -1;
        } else {
          return 1;
        }
      }
    };
    
    @Override
    public abstract int compare(UnderReplicatedBlock o1,
        UnderReplicatedBlock o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }

  int level;
  long blockId;
  long inodeId;
  int expectedReplicas;

  public UnderReplicatedBlock(int level, long blockId, long inodeId, int expectedReplicas) {
    this.level = level;
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.expectedReplicas = expectedReplicas;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }
  
  public long getInodeId() {
    return inodeId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public int getExpectedReplicas() {
    return expectedReplicas;
  }

  public void setExpectedReplicas(int expectedReplicas) {
    this.expectedReplicas = expectedReplicas;
  }
  
  @Override
  public String toString() {
    return "UnderReplicatedBlock{" + "level=" + level + ", blockId=" + blockId +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final UnderReplicatedBlock other = (UnderReplicatedBlock) obj;
    if (this.blockId != other.blockId) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (int) (this.blockId ^ (this.blockId >>> 32));
    return hash;
  }

}
