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

public class CorruptReplica extends ReplicaBase {

  public static enum Finder implements FinderType<CorruptReplica> {

    ByINodeId,
    ByINodeIds,
    ByBlockIdAndINodeId;

    @Override
    public Class getType() {
      return CorruptReplica.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  private String reason;

  public CorruptReplica(int sid, long blockId, long inodeId, String reason) {
    super(sid, blockId, inodeId);
    this.reason = reason;
  }

  public String getReason() {
    return reason;
  }
}
