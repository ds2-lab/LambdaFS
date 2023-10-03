ALTER TABLE `hdfs_aces` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_block_checksum` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_block_infos` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_block_lookup_table` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_cached_block` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_corrupt_replicas` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_encoding_status` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_encoding_status` MODIFY COLUMN `parity_inode_id` bigint(20);

ALTER TABLE `hdfs_excess_replicas` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_inode_attributes` MODIFY COLUMN `inodeId` bigint(20);

ALTER TABLE `hdfs_inodes` MODIFY COLUMN `partition_id` bigint(20);

ALTER TABLE `hdfs_inodes` MODIFY COLUMN `parent_id` bigint(20);

ALTER TABLE `hdfs_inodes` MODIFY COLUMN `id` bigint(20);

ALTER TABLE `hdfs_inmemory_file_inode_data` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_invalidated_blocks` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_ondisk_large_file_inode_data` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_ondisk_medium_file_inode_data` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_metadata_log` MODIFY COLUMN `dataset_id` bigint(20);

ALTER TABLE `hdfs_metadata_log` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_metadata_log` MODIFY COLUMN `inode_partition_id` bigint(20);

ALTER TABLE `hdfs_metadata_log` MODIFY COLUMN `inode_parent_id` bigint(20);

ALTER TABLE `hdfs_inode_dataset_lookup` MODIFY COLUMN `dataset_id` bigint(20);

ALTER TABLE `hdfs_inode_dataset_lookup` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_pending_blocks` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_quota_update` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_replicas` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_replica_under_constructions` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_ondisk_small_file_inode_data` MODIFY COLUMN `inode_id` bigint(20);

ALTER TABLE `hdfs_under_replicated_blocks` MODIFY COLUMN `inode_id` bigint(20);

CREATE TABLE `hdfs_active_block_reports` (
  `dn_address` varchar(128) NOT NULL,
  `nn_id` bigint(20) DEFAULT NULL,
  `start_time` bigint(20) DEFAULT NULL,
  `num_blocks` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`dn_address`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_inodes` DROP COLUMN `client_node`;