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
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.transaction.context.EntityContext;

import java.util.Collection;
import java.util.List;

public interface INodeDataAccess<T> extends EntityDataAccess {
  
  T findInodeByIdFTIS(long inodeId) throws StorageException;

  Collection<T> findInodesByIdsFTIS(long[] inodeId) throws StorageException;
  
  List<T> findInodesByParentIdFTIS(long parentId) throws StorageException;

  List<T> findInodesByParentIdAndPartitionIdPPIS(long parentId, long partitionId) throws StorageException;

  List<ProjectedINode> findInodesPPISTx(long parentId, long partitionId, EntityContext.LockMode lock)
      throws StorageException;

  List<ProjectedINode> findInodesFTISTx(long parentId, EntityContext.LockMode lock)
      throws StorageException;

  T findInodeByNameParentIdAndPartitionIdPK(String name, long parentId, long partitionId)
      throws StorageException;

  List<T> getINodesPkBatched(String[] names, long[] parentIds, long[] partitionIds)
      throws StorageException;

  List<T> lockInodesUsingPkBatchTx(String[] names, long[] parentIds, long[] partitionIds, EntityContext.LockMode lock)
          throws StorageException;

  List<INodeIdentifier> getAllINodeFiles(long startId, long endId)
      throws StorageException;
  
  boolean haveFilesWithIdsGreaterThan(long id) throws StorageException;
  
  boolean haveFilesWithIdsBetween(long startId, long endId)
      throws StorageException;
  
  long getMinFileId() throws StorageException;
  
  long getMaxFileId() throws StorageException;
  
  int countAllFiles() throws StorageException;
  
  void prepare(Collection<T> removed, Collection<T> newed,
      Collection<T> modified) throws StorageException;

  int countAll() throws StorageException;
  
  boolean hasChildren(long parentId, boolean areChildrenRandomlyPartitioned) throws StorageException;
  
  List<T> allINodes() throws StorageException; // only for testing

  void deleteInode(String name)throws StorageException; // only for testing

  void updateLogicalTime(Collection<INodeMetadataLogEntry> logEntries) throws StorageException;

  int countSubtreeLockedInodes() throws StorageException; // only for testing
  
  public long getMaxId() throws StorageException;
}
