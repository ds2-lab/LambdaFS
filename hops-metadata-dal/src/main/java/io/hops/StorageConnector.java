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
package io.hops;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

import java.util.Properties;

public interface StorageConnector<T> {

  public void setConfiguration(Properties conf) throws StorageException;

  public <T> T obtainSession() throws StorageException;

  public void beginTransaction() throws StorageException;

  public void commit() throws StorageException;

  public void rollback() throws StorageException;

  public boolean formatAllStorageNonTransactional() throws StorageException;
  
  public boolean formatYarnStorageNonTransactional() throws StorageException;

  public boolean formatHDFSStorageNonTransactional() throws StorageException;

  public boolean formatStorage() throws StorageException;
  
  public boolean formatYarnStorage() throws StorageException;

  public boolean formatHDFSStorage() throws StorageException;

  public boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException;
    
  public boolean isTransactionActive() throws StorageException;

  public void stopStorage() throws StorageException;

  public void readLock() throws StorageException;

  public void writeLock() throws StorageException;

  public void readCommitted() throws StorageException;

  public void setPartitionKey(Class className, Object key)
      throws StorageException;
  
  public void flush() throws StorageException;
  
   public String getClusterConnectString();

  public String getDatabaseName();
  
  public void returnSession(boolean error) throws StorageException;
}
