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
package io.hops.metadata.hdfs.entity;

public class Storage {
  private final int storage_id;
  private final String host_id;
  private final int storage_type;
  private final String state;
  
  public Storage(int storage_id, String host_id, int storage_type, String state) {
    this.storage_id = storage_id;
    this.host_id = host_id;
    this.storage_type = storage_type;
    this.state = state;
  }

  public int getStorageID() {
    return storage_id;
  }

  public String getHostID() {
    return host_id;
  }

  public int getStorageType() {
    return storage_type;
  }
  
  public String getState() {
    return state;
  }
}
