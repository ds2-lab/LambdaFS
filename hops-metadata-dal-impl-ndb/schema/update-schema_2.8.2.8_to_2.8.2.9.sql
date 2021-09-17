ALTER TABLE `hdfs_inodes` CHANGE `num_xattrs` `num_user_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `num_sys_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_xattrs` MODIFY COLUMN `value` varbinary(13500) DEFAULT "";

CREATE TABLE `hdfs_encryption_zone` (
  `inode_id` bigint(20) NOT NULL,
  `zone_info` VARBINARY(13500),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1';

CREATE TABLE `hdfs_file_provenance_log` (
  `inode_id` bigint(20) NOT NULL,
  `inode_operation` varchar(45) NOT NULL,
  `io_logical_time` int(11) NOT NULL,
  `io_timestamp` bigint(20) NOT NULL,
  `io_app_id` varchar(45) NOT NULL,
  `io_user_id` int(11) NOT NULL,
  `tb` varchar(50) NOT NULL,
  `i_partition_id` bigint(20) NOT NULL,
  `project_i_id` bigint(20) NOT NULL,
  `dataset_i_id` bigint(20) NOT NULL,
  `parent_i_id` bigint(20) NOT NULL,
  `i_name` varchar(255) NOT NULL,
  `project_name` varchar(255) NOT NULL,
  `dataset_name` varchar(255) NOT NULL,
  `i_p1_name` varchar(255) NOT NULL,
  `i_p2_name` varchar(255) NOT NULL,
  `i_parent_name` varchar(255) NOT NULL,
  `io_user_name` varchar(100) NOT NULL,
  `i_xattr_name` varchar(255) NOT NULL,
  `io_logical_time_batch` int(11) NOT NULL,
  `io_timestamp_batch` bigint(20) NOT NULL,
  `ds_logical_time` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`, `inode_operation`, `io_logical_time`, `io_timestamp`, `io_app_id`, `io_user_id`, `tb`),
  KEY `io_logical_time` (`io_logical_time` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `yarn_app_provenance_log` (
  `id` varchar(45) NOT NULL,
  `state` varchar(45) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  `name` varchar(200) NOT NULL,
  `user` varchar(100) NOT NULL,
  `submit_time` bigint(20) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `finish_time` bigint(20) NOT NULL,
  PRIMARY KEY (`id`,`state`,`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `hdfs_file_provenance_xattrs_buffer` (
  `inode_id` bigint(20) NOT NULL,
  `namespace` tinyint(4) NOT NULL,
  `name` varchar(255) COLLATE latin1_general_cs NOT NULL,
  `inode_logical_time` int(11) NOT NULL,
  `value` varbinary(13500) DEFAULT "",
  PRIMARY KEY (`inode_id`,`namespace`,`name`,`inode_logical_time`),
  INDEX `xattr_versions` (`inode_id`, `namespace`, `name` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
