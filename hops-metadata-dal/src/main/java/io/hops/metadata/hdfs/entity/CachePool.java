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

public class CachePool {
  
  private String poolName;
  private String ownerName;
  private String groupName;
  private short mode;
  private long limit;
  private long maxRelativeExpiryMs;
  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  public CachePool(String poolName, String ownerName, String groupName, short mode, long limit, long maxRelativeExpiryMs,
      long bytesNeeded, long bytesCached, long filesNeeded, long filesCached) {
    this.poolName = poolName;
    this.ownerName = ownerName;
    this.groupName = groupName;
    this.mode = mode;
    this.limit = limit;
    this.maxRelativeExpiryMs = maxRelativeExpiryMs;
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
  }

  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public String getGroupName() {
    return groupName;
  }

  public short getMode() {
    return mode;
  }

  public long getLimit() {
    return limit;
  }

  public long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }
  
  
}
