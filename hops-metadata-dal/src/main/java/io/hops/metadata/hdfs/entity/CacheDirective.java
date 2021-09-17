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


public class CacheDirective {
  private Long id;
  private String path;
  private short replication;
  private long expiryTime;
  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;
  private String pool;

  public CacheDirective(Long id, String path, short replication, long expiryTime, long bytesNeeded, long bytesCached,
      long filesNeeded, long filesCached, String pool) {
    this.id = id;
    this.path = path;
    this.replication = replication;
    this.expiryTime = expiryTime;
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
    this.pool = pool;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short replication) {
    this.replication = replication;
  }

  public long getExpiryTime() {
    return expiryTime;
  }

  public void setExpiryTime(long expiryTime) {
    this.expiryTime = expiryTime;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public void setBytesNeeded(long bytesNeeded) {
    this.bytesNeeded = bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public void setBytesCached(long bytesCached) {
    this.bytesCached = bytesCached;
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public void setFilesNeeded(long filesNeeded) {
    this.filesNeeded = filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }

  public void setFilesCached(long filesCached) {
    this.filesCached = filesCached;
  }

  public String getPool() {
    return pool;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }
 
  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof CacheDirective)) {
      return false;
    }
    
    return this.id.equals(((CacheDirective)that).id);
  }

  @Override
  public final int hashCode() {
    return id.hashCode();
  }
}
