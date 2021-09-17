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

public class BlockChecksum {
  private long inodeId;
  private int blockIndex;
  private long checksum;

  public static enum Finder implements FinderType<BlockChecksum> {
    ByKeyTuple,
    ByInodeId;

    @Override
    public Class getType() {
      return BlockChecksum.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByKeyTuple:
          return Annotation.PrimaryKey;
        case ByInodeId:
          return Annotation.PrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  public BlockChecksum() {

  }

  public BlockChecksum(long inodeId, int blockIndex, long checksum) {
    this.inodeId = inodeId;
    this.blockIndex = blockIndex;
    this.checksum = checksum;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(int inodeId) {
    this.inodeId = inodeId;
  }

  public int getBlockIndex() {
    return blockIndex;
  }

  public void setBlockIndex(int blockIndex) {
    this.blockIndex = blockIndex;
  }

  public long getChecksum() {
    return checksum;
  }

  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BlockChecksum that = (BlockChecksum) o;

    if (blockIndex != that.blockIndex) {
      return false;
    }
    if (checksum != that.checksum) {
      return false;
    }
    if (inodeId != that.inodeId) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(inodeId);
    result = 31 * result + blockIndex;
    result = 31 * result + (int) (checksum ^ (checksum >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "BlockChecksum{" +
        "inodeId=" + inodeId +
        ", blockIndex=" + blockIndex +
        ", checksum=" + checksum +
        '}';
  }
}
