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

public class CachedBlock {

  public static enum Finder implements FinderType<CachedBlock> {

    ByBlockIdAndInodeId,
    ByInodeId,
    ByInodeIds,
    ByBlockIdInodeIdAndDatanodeId,
    ByBlockIdsAndINodeIds,
    ByDatanodeId,
    All,
    ByDatanodeAndTypes;
    
    @Override
    public Class getType() {
      return CachedBlock.class;
    }

    @Override
    public FinderType.Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndInodeId:
          return FinderType.Annotation.PrunedIndexScan;
        case ByInodeId:
          return FinderType.Annotation.PrunedIndexScan;
        case ByBlockIdInodeIdAndDatanodeId:
          return FinderType.Annotation.PrimaryKey;
        case ByInodeIds:
          return FinderType.Annotation.BatchedPrunedIndexScan;
        case ByBlockIdsAndINodeIds:
          return Annotation.Batched;
        case ByDatanodeId:
          return FinderType.Annotation.IndexScan;
        case All:
          return FinderType.Annotation.FullTable;
        case ByDatanodeAndTypes:
          return FinderType.Annotation.IndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private final long blockId;
  private final long inodeId;
  private final String datanodeId;
  private final String status;
  private final short replicationAndMark;

  public CachedBlock(long blockId, long inodeId, String datanodeId, String status, short replicationAndMark) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.datanodeId = datanodeId;
    this.status = status;
    this.replicationAndMark = replicationAndMark;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getInodeId() {
    return inodeId;
  }

  public String getDatanodeId() {
    return datanodeId;
  }

  public String getStatus() {
    return status;
  }

  public short getReplicationAndMark() {
    return replicationAndMark;
  }

}
