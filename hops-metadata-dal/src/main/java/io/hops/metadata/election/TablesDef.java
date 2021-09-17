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
package io.hops.metadata.election;

public class TablesDef {
  public abstract static interface LeDescriptorTableDef {
    public static final String ID = "id";
    public static final String COUNTER = "counter";
    public static final String RPC_ADDRESSES = "rpc_addresses";
    public static final String HTTP_ADDRESS = "http_address";
    public static final String PARTITION_VAL = "partition_val";
    public static final String LOCATION_DOMAIN_ID = "location_domain_id";
  }

  public static interface HdfsLeaderTableDef extends LeDescriptorTableDef {
    public static String TABLE_NAME = "hdfs_le_descriptors";
  }

  public static interface YarnLeaderTableDef extends LeDescriptorTableDef {
    public static final String TABLE_NAME = "yarn_le_descriptors";
  }
}
