/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock implements Serializable {

  private static final long serialVersionUID = -8886456043413622280L;
  private ExtendedBlock b; // Should be fine
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfoWithStorage[] locs;
  /** Cached storage ID for each replica */
  private String[] storageIDs;
  /** Cached storage type for each replica, if reported. */
  private StorageType[] storageTypes;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken =
      new Token<>();
  /**
   * List of cached datanode locations
   */
  private DatanodeInfo[] cachedLocs;

  // Used when there are no locations
  private static final DatanodeInfoWithStorage[] EMPTY_LOCS =
      new DatanodeInfoWithStorage[0];

  private byte[] data = null;

  private LocatedBlock() { }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    // By default, startOffset is unknown(-1) and corrupt is false.
    this(b, locs, null, null, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, boolean corrupt) {
    this(b, locs, null, null, startOffset, corrupt, EMPTY_LOCS);
  }
    
  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs, StorageType[] storageTypes) {
    this(b, locs, storageIDs, storageTypes, -1, false, EMPTY_LOCS); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
      StorageType[] storageTypes, long startOffset, boolean corrupt, DatanodeInfo[] cachedLocs) {
      this(b, locs, storageIDs, storageTypes, startOffset, corrupt, new Token<BlockTokenIdentifier>(), cachedLocs);
  }
  
  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
      StorageType[] storageTypes, long startOffset, boolean corrupt, Token<BlockTokenIdentifier> blockToken,
      DatanodeInfo[] cachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs == null) {
      this.locs = EMPTY_LOCS;
    } else {
      this.locs = new DatanodeInfoWithStorage[locs.length];
      for(int i = 0; i < locs.length; i++) {
        DatanodeInfo di = locs[i];
        DatanodeInfoWithStorage storage = new DatanodeInfoWithStorage(di,
            storageIDs != null ? storageIDs[i] : null,
            storageTypes != null ? storageTypes[i] : null);
        this.locs[i] = storage;
      }
    }
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;

    if (cachedLocs == null || cachedLocs.length == 0) {
      this.cachedLocs = EMPTY_LOCS;
    } else {
      this.cachedLocs = cachedLocs;
    }
    this.data = null;
    this.blockToken = blockToken;
  }

  public void setData(byte[] data) {
      if(isPhantomBlock()){
        this.data = data;
      } else {
        throw new UnsupportedOperationException("Can not set data. Not a phantom data block");
      }
  }

  public boolean isPhantomBlock(){
    if (b.getBlockId() < 0) {
        return true;
    }
    return  false;
  }

  public boolean isDataSet(){
     if(isPhantomBlock() && data!= null && data.length > 0) {
       return true;
     }else{
       return false;
     }
  }

  public byte[] getData() {
    String error = null;
    if(isDataSet()) {
      return data;
    }else if(isPhantomBlock()) {
      error = "The file data is not set";
    }else {
      error = "The file data is not stored in the database";
    }
    throw new UnsupportedOperationException(error);
  }


  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  /**
   * Returns the locations associated with this block. The returned array is not
   * expected to be modified. If it is, caller must immediately invoke
   * {@link org.apache.hadoop.hdfs.protocol.LocatedBlock#updateCachedStorageInfo}
   * to update the cached Storage ID/Type arrays.
   */
  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public DatanodeInfo[] getUniqueLocations() {
    HashSet<DatanodeInfo> dns = new HashSet<DatanodeInfo>();
    for(DatanodeInfo dn : this.locs) {
      dns.add(dn);
    }
    return dns.toArray(new DatanodeInfo[0]);
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  public String[] getStorageIDs() {
    return storageIDs;
  }

  /**
   * Updates the cached StorageID and StorageType information. Must be
   * called when the locations array is modified.
   */
  public void updateCachedStorageInfo() {
    if (storageIDs != null) {
      for(int i = 0; i < locs.length; i++) {
        storageIDs[i] = locs[i].getStorageID();
      }
    }
    if (storageTypes != null) {
      for(int i = 0; i < locs.length; i++) {
        storageTypes[i] = locs[i].getStorageType();
      }
    }
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  public void setStartOffset(long value) {
    this.offset = value;
  }

  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  public boolean isCorrupt() {
    return this.corrupt;
  }

   /**
   * Add a the location of a cached replica of the block.
   *
   * @param loc of datanode with the cached replica
   */
  public void addCachedLoc(DatanodeInfo loc) {
    List<DatanodeInfo> cachedList = Lists.newArrayList(cachedLocs);
    if (cachedList.contains(loc)) {
      return;
    }
    // Try to re-use a DatanodeInfo already in loc
    for (DatanodeInfoWithStorage di : locs) {
      if (loc.equals(di)) {
        cachedList.add(di);
        cachedLocs = cachedList.toArray(cachedLocs);
        return;
      }
    }
    // Not present in loc, add it and go
    cachedList.add(loc);
    cachedLocs = cachedList.toArray(cachedLocs);
  }

  /**
   * @return Datanodes with a cached block replica
   */
  public DatanodeInfo[] getCachedLocations() {
    return cachedLocs;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "; locs=" + Arrays.asList(locs)
        + "}";
  }

  public final static Comparator<LocatedBlock> blockIdComparator =
      new Comparator<LocatedBlock>() {

        @Override
        public int compare(LocatedBlock locatedBlock,
            LocatedBlock locatedBlock2) {
          if (locatedBlock.getBlock().getBlockId() <
              locatedBlock2.getBlock().getBlockId()) {
            return -1;
          }
          if (locatedBlock.getBlock().getBlockId() >
              locatedBlock2.getBlock().getBlockId()) {
            return 1;
          }
          return 0;
        }
      };
}
