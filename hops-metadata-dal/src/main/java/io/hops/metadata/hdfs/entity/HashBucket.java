/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.hdfs.entity;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;

public class HashBucket {

  public enum Finder implements FinderType<HashBucket> {
    ByStorageIdAndBucketId,
    ByStorageId;
  
    @Override
    public Class getType() {
      return HashBucket.class;
    }
  
    @Override
    public Annotation getAnnotated() {
      switch (this){
        case ByStorageIdAndBucketId:
          return Annotation.PrimaryKey;
        case ByStorageId:
          return Annotation.IndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  private int storageId;
  private int bucketId;
  private byte[] hash;
  
  public HashBucket(int storageId, int bucketId, byte[] hash) {
    this.storageId = storageId;
    this.bucketId = bucketId;
    this.hash = hash;
  }
  
  public int getBucketId() {
    return bucketId;
  }
  
  public int getStorageId() {
    return storageId;
  }
  
  public byte[] getHash() {
    return hash;
  }
  
  public void setHash(byte[] hash)
      throws TransactionContextException, StorageException {
    this.hash = hash;
    EntityManager.update(this);
  }

  public static class PrimaryKey {
    private int bucketId;
    private int storageId;
    
    public PrimaryKey(int storageId, int bucketId) {
      this.storageId = storageId;
      this.bucketId = bucketId;
    }
    
    public int getBucketId() {
      return bucketId;
    }
    
    public int getStorageId() {
      return storageId;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof PrimaryKey) {
        PrimaryKey otherPK = (PrimaryKey) o;
        return (bucketId == otherPK.getBucketId() &&
            storageId == otherPK.getStorageId());
      }
      return false;
    }

    @Override
    public int hashCode() {
      //TODO: Do we need to implement stronger hash code?
      //Collisions not dangerous, only used for HashMap key.
      return bucketId * storageId;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (byte b : hash) {
        sb.append(String.format("%02X ", b));
    }
    return "HashBucket{" +
            "storageId=" + storageId +
            ", bucketId=" + bucketId +
            ", hash=" +  sb.toString() +
            '}';
  }
}
