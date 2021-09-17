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
import java.util.List;
import java.util.Map;

public interface InvalidateBlockDataAccess<T> extends EntityDataAccess {

  int countAll() throws StorageException;

  List<T> findInvalidatedBlockByStorageId(int storageId)
      throws StorageException;

  Map<Long,Long> findInvalidatedBlockBySidUsingMySQLServer(int sid)
      throws StorageException;

  List<T> findInvalidatedBlocksByBlockId(long bid, long inodeId) throws StorageException;
  
  List<T> findInvalidatedBlocksByINodeId(long inodeId) throws StorageException;

  List<T> findInvalidatedBlocksByINodeIds(long[] inodeIds) throws StorageException;
  
  List<T> findAllInvalidatedBlocks() throws StorageException;

  List<T> findInvalidatedBlocksbyPKS(long[] blockIds, long[] inodesIds, int[] storageIds) throws StorageException;

  T findInvBlockByPkey(long blockId, int sid, long inodeId) throws StorageException;

  void prepare(Collection<T> removed, Collection<T> newed, Collection<T> modified) throws StorageException;

  void removeAll() throws StorageException;

  void removeAllByStorageId(int sid) throws StorageException;

  void removeByBlockIdAndStorageId(long blockId, int sid) throws StorageException;
}
