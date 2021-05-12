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

import java.util.List;

public class PendingBlockInfo {

  private long blockId;
  private long inodeId;
  private long timeStamp;
  private List<String> targets;

  public PendingBlockInfo(long blockId, long inodeId, long timestamp,
      List<String> targets) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.timeStamp = timestamp;
    this.targets = targets;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public List<String> getTargets() {
    return targets;
  }

  public void setTargets(List<String> targets) {
    this.targets = targets;
  }

  public void setInodeId(int inodeId) {
    this.inodeId = inodeId;
  }
  
}
