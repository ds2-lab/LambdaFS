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

import java.util.Comparator;
import java.util.Objects;

/**
 * This class holds the information of one replica of a block in one datanode.
 */
public class Replica extends ReplicaBase {

  public static enum Finder implements FinderType<Replica> {

    ByBlockIdAndINodeId,
    ByINodeId,
    ByINodeIds,
    ByBlockIdAndStorageId;

    @Override
    public Class getType() {
      return Replica.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeId:
          return Annotation.PrunedIndexScan;
        case ByBlockIdAndStorageId:
          return Annotation.IndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  public static enum Order implements Comparator<Replica> {

    ByStorageId() {
      @Override
      public int compare(Replica o1, Replica o2) {
        return Integer.valueOf(o1.getStorageId()).compareTo(
            o2.getStorageId
                ());
      }
    }
  }

  private int bucketId;

  /**
   * @return the hash bucket this block is assigned to
   */
  public int getBucketId(){
    return bucketId;
  }

  public Replica(int storageId, long blockId, long inodeId, int bucketId) {
    super(storageId, blockId, inodeId);
    this.bucketId = bucketId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageId,inodeId,blockId,bucketId);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    Replica replica = (Replica) o;

    return bucketId==replica.bucketId;
  }
  
  @Override
  public int compareTo(ReplicaBase t) {
    if (t == null) {
      return 1;
    }

    int compVal = super.compareTo(t);
    if(compVal != 0){
      return compVal;
    }

    if ( t instanceof  Replica){
      compVal = new Integer(bucketId).compareTo(((Replica)t).bucketId);
      if(compVal != 0){
        return  compVal;
      }
    }

    return 0;
  }
}
