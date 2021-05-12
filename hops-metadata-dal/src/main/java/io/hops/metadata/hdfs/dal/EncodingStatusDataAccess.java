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

public interface EncodingStatusDataAccess<T> extends EntityDataAccess {

  void add(T status) throws StorageException;

  void update(T status) throws StorageException;

  void delete(T status) throws StorageException;

  T findByInodeId(long inodeId) throws StorageException;

  Collection<T> findByInodeIds(Collection<Long> inodeIds) throws StorageException;
  
  T findByParityInodeId(long inodeId) throws StorageException;

  Collection<T> findByParityInodeIds(List<Long> inodeIds) throws StorageException;
  
  Collection<T> findRequestedEncodings(int limit) throws StorageException;

  int countRequestedEncodings() throws StorageException;

  Collection<T> findRequestedRepairs(int limit) throws StorageException;

  int countRequestedRepairs() throws StorageException;

  Collection<T> findActiveEncodings() throws StorageException;

  int countActiveEncodings() throws StorageException;

  Collection<T> findEncoded(int limit) throws StorageException;

  int countEncoded() throws StorageException;

  Collection<T> findActiveRepairs() throws StorageException;

  int countActiveRepairs() throws StorageException;

  Collection<T> findRequestedParityRepairs(int limit) throws StorageException;

  int countRequestedParityRepairs() throws StorageException;

  Collection<T> findActiveParityRepairs() throws StorageException;

  int countActiveParityRepairs() throws StorageException;

  void setLostBlockCount(int n);

  int getLostBlockCount();

  void setLostParityBlockCount(int n);

  int getLostParityBlockCount();

  Collection<T> findDeleted(int limit) throws StorageException;

  Collection<T> findRevoked() throws StorageException;
}
