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

public interface CachedBlockDataAccess<T> extends EntityDataAccess {

  void prepare(Collection<T> removed, Collection<T> newed, Collection<T> modified) throws StorageException;

  T find(long blockId, long inodeId, String datanodeId) throws StorageException;
  
  List<T> findCachedBlockById(long blockId) throws StorageException;
  
  List<T> findCachedBlockByINodeId(long inodeId) throws StorageException;
  
  List<T> findCachedBlockByINodeIds(long[] inodeIds) throws StorageException;
  
  List<T> findByIds(long[] blockIds, long[] inodeIds, String datanodeId) throws StorageException;
  
  List<T> findCachedBlockByDatanodeId(String datanodeId) throws StorageException;
  
  List<T> findAll() throws StorageException;
  
}
