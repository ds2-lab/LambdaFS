CREATE TABLE `yarn_conf_mutation` (
  `index` int(11) NOT NULL,
  `mutation` varbinary(13000) NOT NULL,
  PRIMARY KEY (`index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `yarn_conf` (
  `index` int(11) NOT NULL,
  `conf` blob NOT NULL,
  PRIMARY KEY (`index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_provided` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_provided` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_provided` bigint(20) NOT NULL DEFAULT '-1';

DROP TABLE `yarn_containerid_toclean`;

DROP TABLE `yarn_container_to_signal`;

DROP TABLE `yarn_container_to_decrease`;

DROP TABLE `yarn_containerstatus`;

DROP TABLE `yarn_rmnode_applications`;

DROP TABLE `yarn_nextheartbeat`;

DROP TABLE `yarn_pendingevents`;

DROP TABLE `yarn_resource`;

DROP TABLE `yarn_rms_load`;

DROP TABLE `yarn_rmnode`;

DROP TABLE `yarn_updatedcontainerinfo`;

DROP TABLE `yarn_containers_logs`;

DROP TABLE `yarn_containers_checkpoint`;

insert into hdfs_variables (id, value) select 37, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 38, "" where (select count(*) from hdfs_variables)>0;

ALTER TABLE `hdfs_xattrs`
ADD COLUMN `num_parts` smallint(6) NOT NULL DEFAULT '1',
ADD COLUMN `index` smallint(6) NOT NULL DEFAULT '0',
DROP PRIMARY KEY,
ADD PRIMARY KEY (`inode_id`,`namespace`,`name`, `index`),
COMMENT = 'NDB_TABLE=READ_BACKUP=1';

ALTER TABLE `hdfs_file_provenance_xattrs_buffer`
ADD COLUMN `num_parts` smallint(6) NOT NULL DEFAULT '1',
ADD COLUMN `index` smallint(6) NOT NULL DEFAULT '0',
DROP PRIMARY KEY,
ADD PRIMARY KEY (`inode_id`,`namespace`,`name`,`inode_logical_time`, `index`),
COMMENT = 'NDB_TABLE=READ_BACKUP=1';

ALTER TABLE `hdfs_metadata_log`
ADD COLUMN `inode_partition_id` bigint(20) DEFAULT NULL,
ADD COLUMN `inode_parent_id` bigint(20) DEFAULT NULL,
ADD COLUMN `inode_name` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',
COMMENT = 'NDB_TABLE=READ_BACKUP=1';

ALTER TABLE `hdfs_file_provenance_log`
ADD COLUMN `i_xattr_num_parts` smallint(6) NOT NULL DEFAULT '1',
COMMENT = 'NDB_TABLE=READ_BACKUP=1';

alter table hdfs_leases add column `count` int(11) NOT NULL DEFAULT 0;

CREATE TABLE `hdfs_lease_creation_locks` (  
  `id` int(11) NOT NULL,                   
  PRIMARY KEY (`id`)                       
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

TRUNCATE TABLE hdfs_retry_cache_entry;

ALTER TABLE hdfs_retry_cache_entry ADD (`epoch` bigint(20) NOT NULL DEFAULT 0);

ALTER TABLE hdfs_retry_cache_entry DROP PRIMARY KEY;

ALTER TABLE hdfs_retry_cache_entry ADD PRIMARY KEY (`client_id`,`call_id`,`epoch`) PARTITION BY KEY (`epoch`);

insert into hdfs_variables (id, value) select 39, 0x0000000000000000 where (select count(*) from hdfs_variables)>0;
