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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;

/**
 * Identifies a Block uniquely across the block pools
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExtendedBlock implements Serializable {
  private static final long serialVersionUID = 4963220551036194189L;

  private String poolId;
  private Block block;

  public ExtendedBlock() {
    this(null, 0, 0, 0);
  }

  public ExtendedBlock(final ExtendedBlock b) {
    this(b.poolId, new Block(b.block));
  }

  public ExtendedBlock(final String poolId, final long blockId) {
    this(poolId, blockId, 0, 0);
  }

  public ExtendedBlock(String poolId, Block b) {
    this.poolId = poolId;
    this.block = b;
  }

  public ExtendedBlock(final String poolId, final long blkid, final long len,
      final long genstamp) {
    this.poolId = poolId;
    block = new Block(blkid, len, genstamp);
  }

  public String getBlockPoolId() {
    return poolId;
  }

  /**
   * Returns the block file name for the block
   */
  public String getBlockName() {
    return block.getBlockName();
  }

  public long getNumBytes() {
    return block.getNumBytes();
  }

  public long getBlockId() {
    return block.getBlockId();
  }

  public long getGenerationStamp() {
    return block.getGenerationStamp();
  }

  public void setBlockId(final long bid) {
    block.setBlockIdNoPersistance(bid);
  }

  public void setGenerationStamp(final long genStamp) {
    block.setGenerationStampNoPersistance(genStamp);
  }

  public void setNumBytes(final long len) {
    block.setNumBytesNoPersistance(len);
  }

  public void set(String poolId, Block blk) {
    this.poolId = poolId;
    this.block = blk;
  }

  public static Block getLocalBlock(final ExtendedBlock b) {
    return b == null ? null : b.getLocalBlock();
  }

  public Block getLocalBlock() {
    return block;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtendedBlock)) {
      return false;
    }
    ExtendedBlock b = (ExtendedBlock) o;
    return b.block.equals(block) && b.poolId.equals(poolId);
  }

  @Override // Object
  public int hashCode() {
    int result = 31 + poolId.hashCode();
    return (31 * result + block.hashCode());
  }

  @Override // Object
  public String toString() {
    return poolId + ":" + block;
  }
}
