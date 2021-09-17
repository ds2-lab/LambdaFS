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
import java.util.Map;

public class QuotaUpdate {
  private int id;
  private long inodeId;
  private long namespaceDelta;
  private long storageSpaceDelta;
  private Map<StorageType, Long> typeSpaces;
  
  public enum StorageType {
    DISK,
    SSD,
    RAID5,
    ARCHIVE,
    DB,
    PROVIDED;
  }
  
  public static enum Finder implements FinderType<QuotaUpdate> {
    ByINodeId, ByKey;

    @Override
    public Class getType() {
      return QuotaUpdate.class;
    }

    @Override
    public Annotation getAnnotated() {
      return Annotation.PrunedIndexScan;
    }
  }

  public QuotaUpdate(int id, long inodeId, long namespaceDelta,
      long diskspaceDelta, Map<StorageType, Long> typeSpaces) {
    this.id = id;
    this.inodeId = inodeId;
    this.namespaceDelta = namespaceDelta;
    this.storageSpaceDelta = diskspaceDelta;
    this.typeSpaces = typeSpaces;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public long getNamespaceDelta() {
    return namespaceDelta;
  }

  public void setNamespaceDelta(int namespaceDelta) {
    this.namespaceDelta = namespaceDelta;
  }

  public long getStorageSpaceDelta() {
    return storageSpaceDelta;
  }

  public void setStorageSpaceDelta(long diskspaceDelta) {
    this.storageSpaceDelta = diskspaceDelta;
  }

  public Map<StorageType, Long> getTypeSpaces() {
    return typeSpaces;
  }

  public void setTypeSpaces(Map<StorageType, Long> typeSpaces) {
    this.typeSpaces = typeSpaces;
  }

  @Override
  public String toString() {
    return "QuotaUpdate{" +
        "id=" + id +
        ", inodeId=" + inodeId +
        ", namespaceDelta=" + namespaceDelta +
        ", diskspaceDelta=" + storageSpaceDelta +
        '}';
  }
}
