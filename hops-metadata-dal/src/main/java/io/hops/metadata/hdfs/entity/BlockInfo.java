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

public class BlockInfo {

  private long blockId;
  private int blockIndex;
  private long inodeId;
  private long numBytes;
  private long generationStamp;
  private int blockUCState;
  private long timeStamp;
  private int primaryNodeIndex;
  private long blockRecoveryId;
  private long truncateBlockNumBytes = -1;
  private long truncateBlockGenerationStamp = -1;

  public BlockInfo(long blockId, int blockIndex, long inodeId, long numBytes,
      long generationStamp, int blockUnderConstructionState, long timeStamp) {
    this.blockId = blockId;
    this.blockIndex = blockIndex;
    this.inodeId = inodeId;
    this.numBytes = numBytes;
    this.generationStamp = generationStamp;
    this.blockUCState = blockUnderConstructionState;
    this.timeStamp = timeStamp;
  }

  public BlockInfo(long blockId, int blockIndex, long inodeId, long numBytes,
      long generationStamp, int blockUnderConstructionState, long timeStamp,
      int primaryNodeIndex, long blockRecoveryId, long truncateBlockNumBytes,
      long truncateBlockGenerationStamp) {
    this(blockId, blockIndex, inodeId, numBytes, generationStamp,
        blockUnderConstructionState, timeStamp);
    this.primaryNodeIndex = primaryNodeIndex;
    this.blockRecoveryId = blockRecoveryId;
    this.truncateBlockGenerationStamp = truncateBlockGenerationStamp;
    this.truncateBlockNumBytes = truncateBlockNumBytes;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getBlockIndex() {
    return blockIndex;
  }

  public long getInodeId() {
    return inodeId;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public int getBlockUCState() {
    return blockUCState;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public int getPrimaryNodeIndex() {
    return primaryNodeIndex;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public void setBlockIndex(int blockIndex) {
    this.blockIndex = blockIndex;
  }

  public void setInodeId(int inodeId) {
    this.inodeId = inodeId;
  }

  public void setNumBytes(long numBytes) {
    this.numBytes = numBytes;
  }

  public void setGenerationStamp(long generationStamp) {
    this.generationStamp = generationStamp;
  }

  public void setBlockUCState(int blockUCState) {
    this.blockUCState = blockUCState;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public void setPrimaryNodeIndex(int primaryNodeIndex) {
    this.primaryNodeIndex = primaryNodeIndex;
  }

  public void setBlockRecoveryId(long blockRecoveryId) {
    this.blockRecoveryId = blockRecoveryId;
  }

  public long getTruncateBlockNumBytes() {
    return truncateBlockNumBytes;
  }

  public void setTruncateBlockNumBytes(long truncateBlockNumBytes) {
    this.truncateBlockNumBytes = truncateBlockNumBytes;
  }

  public long getTruncateBlockGenerationStamp() {
    return truncateBlockGenerationStamp;
  }

  public void setTruncateBlockGenerationStamp(long truncateBlockGenerationStamp) {
    this.truncateBlockGenerationStamp = truncateBlockGenerationStamp;
  }
}
