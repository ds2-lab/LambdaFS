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

public interface ReplicaDataAccess<T> extends EntityDataAccess {

  List<T> findReplicasById(long blockId, long inodeId) throws StorageException;
  
  List<T> findReplicasByINodeId(long inodeId) throws StorageException;
  
  List<T> findReplicasByINodeIds(long[] inodeIds) throws StorageException;
  
  Map<Long, Long> findBlockAndInodeIdsByStorageId(int storageId) throws StorageException;
  
  Map<Long, Long> findBlockAndInodeIdsByStorageIdAndBucketId(int
      storageId, int bucketId) throws StorageException;
  
  boolean hasBlocksWithIdGreaterThan(int storageId, long from) throws StorageException;
  
  int countAllReplicasForStorageId(int sid) throws StorageException;
  
  void prepare(Collection<T> removed, Collection<T> newed,
      Collection<T> modified) throws StorageException;
  
  Map<Long,Long> findBlockAndInodeIdsByStorageIdAndBucketIds (int sId,
      List<Integer> mismatchedBuckets) throws StorageException;
  
  long findBlockIdAtIndex(int storageId, long index, int maxFetchingSize) throws StorageException;
}