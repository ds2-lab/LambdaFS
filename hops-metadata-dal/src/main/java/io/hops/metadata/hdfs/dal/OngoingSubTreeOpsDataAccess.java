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

public interface OngoingSubTreeOpsDataAccess<T> extends EntityDataAccess {

  T findByPath(String path) throws StorageException;

  Collection<T> findByPathsByPrefix(String prefix) throws StorageException;

  Collection<T> allOpsByNN(long nnID) throws StorageException;

  Collection<T> allOpsToRecoverAsync() throws StorageException;

  //only for testing
  Collection<T> allOps() throws StorageException;

  void prepare(Collection<T> removed, Collection<T> newed,
      Collection<T> modified) throws StorageException;
}
