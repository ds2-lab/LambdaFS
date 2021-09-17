ALTER TABLE `hdfs_storages` ADD COLUMN `state` varchar(100) NOT NULL DEFAULT 'NORMAL';

CREATE TABLE `hdfs_cache_directive` (
  `id` BIGINT NOT NULL,
  `replication` SMALLINT NOT NULL,
  `expirytime` BIGINT NOT NULL,
  `bytes_needed` BIGINT NOT NULL,
  `bytes_cached` BIGINT NOT NULL,
  `files_needed` BIGINT NOT NULL,
  `files_cached` BIGINT NOT NULL,
  `pool` VARCHAR(250) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `pool` (`pool` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `hdfs_cache_directive_path` (
  `id` BIGINT NOT NULL,
  `index` SMALLINT NOT NULL,
  `value` VARCHAR(2500) NOT NULL,
  PRIMARY KEY (`id`, `index`),
  CONSTRAINT id FOREIGN KEY (id) 
  REFERENCES hdfs_cache_directive(id) ON DELETE CASCADE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `hdfs_cache_pool` (
  `pool_name` VARCHAR(250) NOT NULL,
  `owner_name` VARCHAR(250) NOT NULL,
  `group_name` VARCHAR(250) NOT NULL,
  `mode` SMALLINT NOT NULL,
  `limit` BIGINT NOT NULL,
  `max_relative_expiry_ms` BIGINT NOT NULL,
  `bytes_needed` BIGINT NOT NULL,
  `bytes_cached` BIGINT NOT NULL,
  `files_needed` BIGINT NOT NULL,
  `files_cached` BIGINT NOT NULL,
  PRIMARY KEY (`pool_name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `hdfs_cached_block` (
  `block_id` bigint(20) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `datanode_id` VARCHAR(250) NOT NULL,
  `status` VARCHAR(250) NOT NULL,
  `replication_and_mark` SMALLINT NOT NULL,
  PRIMARY KEY (`block_id`, `inode_id`, `datanode_id`),
  INDEX `inode_id` (`inode_id` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

DROP TABLE `hdfs_misreplicated_range_queue`;

CREATE TABLE `hdfs_misreplicated_range_queue` (
  `nn_id` bigint(20) NOT NULL,
  `start_index` bigint(20) NOT NULL,
  PRIMARY KEY (`nn_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

insert into hdfs_variables (id, value) select 27, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 28, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 29, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 30, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 31, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 32, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 33, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 34, "" where (select count(*) from hdfs_variables)>0;

truncate table hdfs_retry_cache_entry;

