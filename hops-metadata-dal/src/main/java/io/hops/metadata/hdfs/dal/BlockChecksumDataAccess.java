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
package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

import java.util.Collection;

public interface BlockChecksumDataAccess<T> extends EntityDataAccess {

  public static class KeyTuple {
    private long inodeId;
    private int blockIndex;

    public KeyTuple(long inodeId, int blockIndex) {
      this.inodeId = inodeId;
      this.blockIndex = blockIndex;
    }

    public long getInodeId() {
      return inodeId;
    }

    public void setInodeId(long inodeId) {
      this.inodeId = inodeId;
    }

    public int getBlockIndex() {
      return blockIndex;
    }

    public void setBlockIndex(int blockIndex) {
      this.blockIndex = blockIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      KeyTuple keyTuple = (KeyTuple) o;

      if (blockIndex != keyTuple.blockIndex) {
        return false;
      }
      if (inodeId != keyTuple.inodeId) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = Long.hashCode(inodeId);
      result = 31 * result + blockIndex;
      return result;
    }

    @Override
    public String toString() {
      return "KeyTuple{" +
          "inodeId=" + inodeId +
          ", blockIndex=" + blockIndex +
          '}';
    }
  }

  void add(T blockChecksum) throws StorageException;

  void update(T blockChecksum) throws StorageException;

  void delete(T blockChecksum) throws StorageException;

  T find(long inodeId, int blockIndex) throws StorageException;

  Collection<T> findAll(long inodeId) throws StorageException;

  void deleteAll(long inodeId) throws StorageException;
}
