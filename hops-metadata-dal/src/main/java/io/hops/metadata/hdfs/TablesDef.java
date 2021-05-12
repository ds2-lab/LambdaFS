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
package io.hops.metadata.hdfs;

public class TablesDef {

  /**
   * Defines the MySQL NDB table for the DataNodes.
   * This table is used by serverless NameNodes to establish connections with the DataNodes.
   */
  public interface DataNodesTableDef {
    String TABLE_NAME = "datanodes";
    String DATANODE_UUID = "datanode_uuid";
    String HOSTNAME = "hostname";
    String IP_ADDR = "ipaddr";
    String XFER_PORT = "xfer_port";
    String INFO_PORT = "info_port";
    String IPC_PORT = "ipc_port";
  }

  public interface BlockLookUpTableDef {
    String TABLE_NAME = "hdfs_block_lookup_table";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
  }

  public interface StoragesTableDef {
    String TABLE_NAME="hdfs_storages";
    String STORAGE_ID="storage_id";
    String HOST_ID="host_id";
    String STORAGE_TYPE="storage_type";
    String STATE = "state";
  }
  
  public interface INodeTableDef {
    // INode Table Columns
    String TABLE_NAME = "hdfs_inodes";
    String ID = "id";
    String NAME = "name";
    String PARENT_ID = "parent_id";
    String PARTITION_ID = "partition_id";
    String IS_DIR = "is_dir";
    String MODIFICATION_TIME = "modification_time";
    String ACCESS_TIME = "access_time";
    String USER_ID = "user_id";
    String GROUP_ID = "group_id";
    String PERMISSION = "permission";
    String CLIENT_NAME = "client_name";
    String CLIENT_MACHINE = "client_machine";
    String GENERATION_STAMP = "generation_stamp";
    String HEADER = "header";
    String SYMLINK = "symlink";
    String QUOTA_ENABLED = "quota_enabled";
    String UNDER_CONSTRUCTION = "under_construction";
    String SUBTREE_LOCKED = "subtree_locked";
    String SUBTREE_LOCK_OWNER = "subtree_lock_owner";
    String META_ENABLED = "meta_enabled";
    String SIZE = "size";
    String FILE_STORED_IN_DB = "file_stored_in_db";
    String LOGICAL_TIME = "logical_time";
    String STORAGE_POLICY = "storage_policy";
    String CHILDREN_NUM = "children_num";
    String NUM_ACES = "num_aces";
    String NUM_USER_XATTRS = "num_user_xattrs";
    String NUM_SYS_XATTRS = "num_sys_xattrs";
  }

  public interface FileInodeInMemoryData {
    String TABLE_NAME = "hdfs_inmemory_file_inode_data";
    String ID = "inode_id";
    String DATA = "data";
  }

  public interface FileInodeSmallDiskData {
    String TABLE_NAME = "hdfs_ondisk_small_file_inode_data";
    String ID = "inode_id";
    String DATA = "data";
  }

  public interface FileInodeMediumlDiskData {
    String TABLE_NAME = "hdfs_ondisk_medium_file_inode_data";
    String ID = "inode_id";
    String DATA = "data";
  }

  public interface FileInodeLargeDiskData {
    String TABLE_NAME = "hdfs_ondisk_large_file_inode_data";
    String ID = "inode_id";
    String INDEX = "dindex";
    String DATA = "data";
  }

  public interface UsersTableDef {
    String TABLE_NAME = "hdfs_users";
    String ID = "id";
    String NAME = "name";
  }

  public interface GroupsTableDef {
    String TABLE_NAME = "hdfs_groups";
    String ID = "id";
    String NAME = "name";
  }

  public interface UsersGroupsTableDef {
    String TABLE_NAME = "hdfs_users_groups";
    String USER_ID = "user_id";
    String GROUP_ID = "group_id";
  }

  public interface BlockChecksumTableDef {
    String TABLE_NAME = "hdfs_block_checksum";

    String INODE_ID = "inode_id";
    String BLOCK_INDEX = "block_index";
    String CHECKSUM = "checksum";
  }

  public interface CorruptReplicaTableDef {

    String TABLE_NAME = "hdfs_corrupt_replicas";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
    String STORAGE_ID = "storage_id";
    String TIMESTAMP = "timestamp";
    String REASON = "reason";
  }

  public interface DirectoryWithQuotaFeatureTableDef {

    String TABLE_NAME = "hdfs_directory_with_quota_feature";
    String ID = "inodeId";
    String NSQUOTA = "nsquota";
    String SSQUOTA = "ssquota";
    String NSCOUNT = "nscount";
    String STORAGESPACE = "storage_space";
    String TYPESPACE_QUOTA_DISK = "typespace_quota_disk";
    String TYPESPACE_QUOTA_SSD = "typespace_quota_ssd";
    String TYPESPACE_QUOTA_RAID5 = "typespace_quota_raid5";
    String TYPESPACE_QUOTA_ARCHIVE = "typespace_quota_archive";
    String TYPESPACE_QUOTA_DB = "typespace_quota_db";
    String TYPESPACE_QUOTA_PROVIDED = "typespace_quota_provided";
    String TYPESPACE_USED_DISK = "typespace_used_disk";
    String TYPESPACE_USED_SSD = "typespace_used_ssd";
    String TYPESPACE_USED_RAID5 = "typespace_used_raid5";
    String TYPESPACE_USED_ARCHIVE = "typespace_used_archive";
    String TYPESPACE_USED_DB = "typespace_used_db";
    String TYPESPACE_USED_PROVIDED = "typespace_used_provided";
  }

  public interface ExcessReplicaTableDef {

    String TABLE_NAME = "hdfs_excess_replicas";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
    String PART_KEY = "part_key";
    String STORAGE_ID = "storage_id";
    String STORAGE_IDX = "storage_idx";
  }

  public interface BlockInfoTableDef {

    String TABLE_NAME = "hdfs_block_infos";
    String BLOCK_ID = "block_id";
    String BLOCK_INDEX = "block_index";
    String INODE_ID = "inode_id";
    String NUM_BYTES = "num_bytes";
    String GENERATION_STAMP = "generation_stamp";
    String BLOCK_UNDER_CONSTRUCTION_STATE =
        "block_under_construction_state";
    String TIME_STAMP = "time_stamp";
    String PRIMARY_NODE_INDEX = "primary_node_index";
    String BLOCK_RECOVERY_ID = "block_recovery_id";
    String TRUNCATE_BLOCK_NUM_BYTES = "truncate_block_num_bytes";
    String TRUNCATE_BLOCK_GENERATION_STAMP = "truncate_block_generation_stamp";
  }

  public interface EncodingStatusTableDef {
    String TABLE_NAME = "hdfs_encoding_status";

    // Erasure coding table columns
    String INODE_ID = "inode_id";
    String PARITY_INODE_ID = "parity_inode_id";
    String STATUS = "status";
    String PARITY_STATUS = "parity_status";
    String CODEC = "codec";
    String TARGET_REPLICATION = "target_replication";
    String STATUS_MODIFICATION_TIME =
        "status_modification_time";
    String PARITY_STATUS_MODIFICATION_TIME =
        "parity_status_modification_time";
    String PARITY_FILE_NAME = "parity_file_name";
    String LOST_BLOCKS = "lost_blocks";
    String LOST_PARITY_BLOCKS = "lost_parity_blocks";
    String LOST_BLOCK_SUM = "lost_block_sum";
    String REVOKED = "revoked";
  }

  public interface LeaseTableDef {
    String TABLE_NAME = "hdfs_leases";
    String HOLDER = "holder";
    String LAST_UPDATE = "last_update";
    String HOLDER_ID = "holder_id";
    String COUNT = "count";
  }

  public interface LeasePathTableDef {
    String TABLE_NAME = "hdfs_lease_paths";
    String HOLDER_ID = "holder_id";
    String PATH = "path";
    String LAST_BLOCK_ID = "last_block_id";
    String PENULTIMATE_BLOCK_ID = "penultimate_block_id";
  }

  public interface LeaseCreationLocksTableDef {
    String TABLE_NAME = "hdfs_lease_creation_locks";
    String ID = "id";
  }

  public interface InvalidatedBlockTableDef {

    String TABLE_NAME = "hdfs_invalidated_blocks";
    String BLOCK_ID = "block_id";
    String STORAGE_ID = "storage_id";
    String STORAGE_IDX = "storage_idx";
    String INODE_ID = "inode_id";
    String GENERATION_STAMP = "generation_stamp";
    String NUM_BYTES = "num_bytes";
  }

  public interface MisReplicatedRangeQueueTableDef {

    String TABLE_NAME = "hdfs_misreplicated_range_queue";
    String NNID = "nn_id";
    String START_INDEX = "start_index";
  }

  public interface PendingBlockTableDef {

    String TABLE_NAME = "hdfs_pending_blocks";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
    String TIME_STAMP = "time_stamp";
    String TARGET = "target";
  }

  public interface ReplicaTableDef {

    String TABLE_NAME = "hdfs_replicas";
    String BLOCK_ID = "block_id";
    String STORAGE_ID = "storage_id";
    String INODE_ID = "inode_id";
    String REPLICA_INDEX = "replica_index";
    String BUCKET_ID = "bucket_id";
  }

  public interface QuotaUpdateTableDef {

    String TABLE_NAME = "hdfs_quota_update";
    String ID = "id";
    String INODE_ID = "inode_id";
    String NAMESPACE_DELTA = "namespace_delta";
    String STORAGE_SPACE_DELTA = "storage_space_delta";
    String TYPESPACE_DELTA_DISK = "typespace_delta_disk";
    String TYPESPACE_DELTA_SSD = "typespace_delta_ssd";
    String TYPESPACE_DELTA_RAID5 = "typespace_delta_raid5";
    String TYPESPACE_DELTA_ARCHIVE = "typespace_delta_archive";
    String TYPESPACE_DELTA_DB = "typespace_delta_db";
    String TYPESPACE_DELTA_PROVIDED = "typespace_delta_provided";

  }

  public interface ReplicaUnderConstructionTableDef {

    String TABLE_NAME = "hdfs_replica_under_constructions";
    String BLOCK_ID = "block_id";
    String STORAGE_ID = "storage_id";
    String INODE_ID = "inode_id";
    String STATE = "state";
    String REPLICA_INDEX = "replica_index";
    String HASH_BUCKET = "bucket_id";
    String CHOSEN_AS_PRIMARY = "chosen_as_primary";
    String GENERATION_STAMP = "generation_stamp";
  }

  public interface SafeBlocksTableDef {

    String TABLE_NAME = "hdfs_safe_blocks";
    String ID = "id";
  }

  public interface StorageIdMapTableDef {
    String TABLE_NAME = "hdfs_storage_id_map";
    String STORAGE_ID = "storage_id";
    String SID = "sid";
  }

  public interface UnderReplicatedBlockTableDef {

    String TABLE_NAME = "hdfs_under_replicated_blocks";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
    String LEVEL = "level";
    String TIMESTAMP = "timestamp";
    String EXPECTEDREPLICAS = "expected_replicas";
  }

  public interface VariableTableDef {

    String TABLE_NAME = "hdfs_variables";
    String ID = "id";
    String VARIABLE_VALUE = "value";
    Integer MAX_VARIABLE_SIZE = 500;
  }

  public interface MetadataLogTableDef {

    String TABLE_NAME = "hdfs_metadata_log";
    String LOOKUP_TABLE_NAME = "hdfs_inode_dataset_lookup";
    String DATASET_ID = "dataset_id";
    String INODE_ID = "inode_id";
    String Logical_TIME = "logical_time";
    String PK1 = "pk1";
    String PK2 = "pk2";
    String PK3 = "pk3";
    String OPERATION = "operation";
    String INODE_PARTITION_ID = "inode_partition_id";
    String INODE_PARENT_ID = "inode_parent_id";
    String INODE_NAME = "inode_name";
  }


  public interface EncodingJobsTableDef {
    String TABLE_NAME = "hdfs_encoding_jobs";
    String JT_IDENTIFIER = "jt_identifier";
    String JOB_ID = "job_id";
    String PATH = "path";
    String JOB_DIR = "job_dir";
  }
  public interface RepairJobsTableDef {
    String TABLE_NAME = "hdfs_repair_jobs";
    String JT_IDENTIFIER = "jt_identifier";
    String JOB_ID = "job_id";
    String PATH = "path";
    String IN_DIR = "in_dir";
    String OUT_DIR = "out_dir";
  }
  
  public interface OnGoingSubTreeOpsDef {
    String TABLE_NAME = "hdfs_on_going_sub_tree_ops";
    String PATH = "path";
    String NAME_NODE_ID = "namenode_id";
    String OP_NAME = "op_name";
    String PARTITION_ID = "partition_id";
    String START_TIME = "start_time";
    String ASYNC_LOCK_RECOVERY_TIME = "async_lock_recovery_time";
    String USER = "user";
    String INODE_ID = "inode_id";
  }

  public interface HashBucketsTableDef {
    String TABLE_NAME = "hdfs_hash_buckets";
    String BUCKET_ID = "bucket_id";
    String STORAGE_ID = "storage_id";
    String HASH = "hash";
  }
  
  public interface AcesTableDef {
    String TABLE_NAME = "hdfs_aces";
    String INODE_ID = "inode_id";
    String INDEX = "index";
    String SUBJECT = "subject";
    String TYPE = "type";
    String IS_DEFAULT = "is_default";
    String PERMISSION = "permission";
  }
  
  public interface RetryCacheEntryTableDef {
    String TABLE_NAME = "hdfs_retry_cache_entry";
    String CLIENTID = "client_id";
    String CALLID = "call_id";
    String PAYLOAD = "payload";
    String EXPIRATION_TIME = "expiration_time";
    String EPOCH = "epoch";
    String STATE = "state";
  }
  
  public interface CacheDirectiveTableDef {
    String TABLE_NAME = "hdfs_cache_directive";
    String ID = "id";
    String REPLICATION = "replication";
    String EXPIRYTIME = "expirytime";
    String BYTES_NEEDED = "bytes_needed";
    String BYTES_CACHED = "bytes_cached";
    String FILES_NEEDED = "files_needed";
    String FILES_CACHED = "files_cached";
    String POOL = "pool";
  }

  public interface CacheDirectivePathTableDef {
    String TABLE_NAME = "hdfs_cache_directive_path";
    String ID = "id";
    String INDEX = "index";
    String VALUE = "value";
  }

  public interface CachePoolTableDef {
    String TABLE_NAME = "hdfs_cache_pool";
    String POOL_NAME = "pool_name";
    String OWNER_NAME = "owner_name";
    String GROUP_NAME = "group_name";
    String MODE = "mode";
    String LIMIT = "limit";
    String MAX_RELATIVE_EXPIRY_MS = "max_relative_expiry_ms";
    String BYTES_NEEDED = "bytes_needed";
    String BYTES_CACHED = "bytes_cached";
    String FILES_NEEDED = "files_needed";
    String FILES_CACHED = "files_cached";
  }
  
  public interface CachedBlockTableDef {
    String TABLE_NAME = "hdfs_cached_block";
    String BLOCK_ID = "block_id";
    String INODE_ID = "inode_id";
    String DATANODE_ID = "datanode_id";
    String STATUS = "status";
    String REPLICATION_AND_MARK = "replication_and_mark";
  }

  public interface ActiveBlockReports {
    String TABLE_NAME = "hdfs_active_block_reports";
    String DN_ADDRESS = "dn_address";
    String NN_ID = "nn_id";
    String NN_ADDRESS = "nn_address";
    String START_TIME = "start_time";
    String NUM_BLOCKS = "num_blocks";
  }
  
  public interface XAttrTableDef {
    String TABLE_NAME = "hdfs_xattrs";
    String INODE_ID = "inode_id";
    String NAMESPACE = "namespace";
    String NAME = "name";
    String VALUE = "value";
    String INDEX = "index";
    String NUM_PARTS = "num_parts";
  }
  
  public interface EncryptionZones {
    String TABLE_NAME = "hdfs_encryption_zone";
    String INODE_ID = "inode_id";
    String ZONE_INFO = "zone_info";
  }

  public interface FileProvenanceTableDef {

    String TABLE_NAME = "hdfs_file_provenance_log";
    String INODE_ID = "inode_id";
    String OPERATION = "inode_operation";
    String LOGICAL_TIME = "io_logical_time";
    String TIMESTAMP = "io_timestamp";
    String APP_ID = "io_app_id";
    String USER_ID = "io_user_id";
    String TIE_BREAKER = "tb";
    String PARTITION_ID = "i_partition_id";
    String PROJECT_ID = "project_i_id";
    String DATASET_ID = "dataset_i_id";
    String PARENT_ID = "parent_i_id";
    String INODE_NAME = "i_name";
    String PROJECT_NAME = "project_name";
    String DATASET_NAME = "dataset_name";
    String P1_NAME = "i_p1_name";
    String P2_NAME = "i_p2_name";
    String PARENT_NAME = "i_parent_name";
    String USER_NAME = "io_user_name";
    String XATTR_NAME = "i_xattr_name";
    String LOGICAL_TIME_BATCH = "io_logical_time_batch";
    String TIMESTAMP_BATCH = "io_timestamp_batch";
    String DS_LOGICAL_TIME = "ds_logical_time";
    String XATTR_NUM_PARTS = "i_xattr_num_parts";
  }
  
  public interface FileProvXAttrBufferTableDef {
    String TABLE_NAME = "hdfs_file_provenance_xattrs_buffer";
    String INODE_ID = "inode_id";
    String NAMESPACE = "namespace";
    String NAME = "name";
    String INODE_LOGICAL_TIME = "inode_logical_time";
    String VALUE = "value";
    String INDEX = "index";
    String NUM_PARTS = "num_parts";
  }
}
