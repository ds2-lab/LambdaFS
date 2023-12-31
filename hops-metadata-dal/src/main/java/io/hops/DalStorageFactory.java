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
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;

import javax.swing.text.html.parser.Entity;
import java.util.Map;
import java.util.Properties;

public interface DalStorageFactory {

  public void setConfiguration(Properties conf)
      throws StorageInitializtionException;

  public StorageConnector getConnector();

  public EntityDataAccess getDataAccess(Class type);
  
  public boolean hasResources(double threshold) throws StorageException;

  public float getResourceMemUtilization() throws StorageException;

  public Map<Class, EntityDataAccess> getDataAccessMap();
}
