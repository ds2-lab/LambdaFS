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
import java.util.Set;

public interface BlockInfoDataAccess<T> extends EntityDataAccess {

  int countAll() throws StorageException;
  
  int countAllCompleteBlocks() throws StorageException;

  T findById(long blockId, long inodeId) throws StorageException;

  List<T> findByInodeId(long inodeId) throws StorageException;
  
  List<T> findByInodeIds(long[] inodeIds) throws StorageException;
  
  List<T> findAllBlocks() throws StorageException;

  List<T> findBlockInfosByStorageId(int storageId) throws StorageException;

  List<T> findBlockInfosByStorageId(int storageId, long from, int size) throws StorageException;
  
  /**
   * Returns a list of all blocks stored on a set of storages.
   *
   * Beware; running this for a big datanode requires a lot of memory
   */
  List<T> findBlockInfosBySids(List<Integer> sids) throws StorageException;

  Set<Long> findINodeIdsByStorageId(int storageId) throws StorageException;
  
  List<T> findByIds(long[] blockIds, long[] inodeIds) throws StorageException;

  boolean existsOnAnyStorage(long inodeId, long blockId, List<Integer> sids) throws StorageException;

  void prepare(Collection<T> removed, Collection<T> newed,
      Collection<T> modified) throws StorageException;
}
