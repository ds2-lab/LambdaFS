/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

public interface XAttrDataAccess<T, P> extends EntityDataAccess {
  
  List<T> getXAttrsByPrimaryKeyBatch(List<P> pks) throws StorageException;
  Collection<T>  getXAttrsByInodeId(long inodeId) throws StorageException;
  int removeXAttrsByInodeId(long inodeId) throws StorageException;
  void prepare(Collection<T> removed, Collection<T> newed,
      Collection<T> modified) throws StorageException;
  int count() throws StorageException;
}
