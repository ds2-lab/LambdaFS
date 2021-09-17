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

public class BlockLookUp implements Comparable<BlockLookUp> {

  private long block_id;
  private long inode_id;

  public BlockLookUp(long block_id, long inode_id) {
    this.block_id = block_id;
    this.inode_id = inode_id;
  }

  public long getBlockId() {
    return block_id;
  }

  public long getInodeId() {
    return inode_id;
  }

  public void setBlockId(long block_id) {
    this.block_id = block_id;
  }

  public void setInodeId(int inode_id) {
    this.inode_id = inode_id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final BlockLookUp other = (BlockLookUp) obj;
    if (this.block_id == other.block_id && this.inode_id == other.inode_id) {
      return true;
    }
    return false;
  }


  @Override
  public int hashCode() {
    int hash = 7;
    hash = 59 * hash + Long.hashCode(this.inode_id);
    hash = 59 * hash + (int) (this.block_id ^ (this.block_id >>> 32));
    return hash;
  }

  @Override
  public int compareTo(BlockLookUp o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
