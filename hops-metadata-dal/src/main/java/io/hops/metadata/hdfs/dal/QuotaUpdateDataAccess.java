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
import io.hops.metadata.hdfs.entity.QuotaUpdate;

import java.util.Collection;
import java.util.List;

public interface QuotaUpdateDataAccess<T> extends EntityDataAccess {

  public void prepare(Collection<T> modified, Collection<T> removed)
      throws StorageException;

  public List<QuotaUpdate> findLimited(int limit) throws StorageException;

  public List<QuotaUpdate> findByInodeId(long inodeId) throws StorageException;
  
  public QuotaUpdate findByKey(int id, long inodeId) throws StorageException;

  public int getCount() throws StorageException;
}
