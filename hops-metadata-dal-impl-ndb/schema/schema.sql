delimiter $$

drop procedure if exists flyway$$

delimiter $$

CREATE PROCEDURE flyway()
BEGIN
	IF EXISTS(SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
			WHERE table_schema = 'hops'
             AND table_name LIKE 'flyway_schema_history')

	THEN
		ALTER TABLE `flyway_schema_history` engine = 'ndb';
	END IF;
END$$

delimiter $$

CALL flyway$$

delimiter $$

-- Used to keep track of the current versions of the various Serverless NameNodes.
-- Basically, NameNodes populate their ActiveNodes lists with this information. When
-- a NameNode gets created, it updates its entry in this table with its NameNode ID.
-- If the NameNode on a particular deployment of the OpenWhisk NameNode function either
-- crashes or its container gets reclaimed or whatever, a new instance will be created.
-- This instance will have a new ID. If the hold instance held any locks, we need a way to
-- determine that the old instance literally doesn't exist anymore. This table serves as a
-- record of the currently-existing NameNodes.
CREATE TABLE `serverless_namenodes` (
    `namenode_id` bigint(20) NOT NULL,      -- The ID of the NameNode object.
    `function_name` varchar(36) NOT NULL,   -- The name of the serverless function in/on which the NN is running.
    `replica_id` varchar(36) NOT NULL,      -- Basically a place-holder for the future if we scale-out deployments.
    `creation_time` bigint(20),             -- When the NameNode instance started running.
    PRIMARY KEY (`namenode_id`, `function_name`), -- Eventually, `replica_id` may be a part of the PK.
    UNIQUE KEY `namenode_idx` (`namenode_id`),
    KEY `function_namex` (`function_name`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `datanodes` (
    `datanode_uuid` varchar(36) NOT NULL,
    `hostname` varchar(253) NOT NULL,
    `ipaddr` varchar(15) NOT NULL,
    `xfer_port` int(11),
    `info_port` int(11),
    `info_secure_port` int(11),
    `ipc_port` int(11),
    `creation_time` bigint(20),
    PRIMARY KEY (`datanode_uuid`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- This table holds individual storage reports. Storage reports will reference datanode storage instances.
CREATE TABLE `storage_reports` (
    `group_id` bigint(20) NOT NULL,       -- DNs typically send several reports in a single heartbeat.
                                          -- We can tell which reports are grouped together by this field.
    `report_id` int(11) NOT NULL,         -- This is how we distinguish between entire groups of storage reports.
    `datanode_uuid` varchar(36) NOT NULL, -- This should refer to a Datanode from the other table.
    `failed` BIT(1) NOT NULL,
    `capacity` bigint(20) NOT NULL,
    `dfsUsed` bigint(20) NOT NULL,
    `remaining` bigint(20) NOT NULL,
    `blockPoolUsed` bigint(20) NOT NULL,
    `datanodeStorageId` varchar(255) NOT NULL, -- This should refer to a given DatanodeStorage from the other table.
    PRIMARY KEY (`group_id`, `report_id`, `datanode_uuid`),
    -- FOREIGN KEY (`datanodeStorageId`) REFERENCES `datanode_storages` (`storage_id`),
    FOREIGN KEY (`datanode_uuid`) REFERENCES `datanodes` (`datanode_uuid`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- This table is used to store DatanodeStorage instances. These instances are referenced by StorageReports.
CREATE TABLE `datanode_storages` (
    `datanode_uuid` varchar(36) NOT NULL,
    `storage_id` varchar(255) NOT NULL,
    `state` int(11) NOT NULL, -- This refers to the State enum. There are 3 possible values.
    `storage_type` int(11) NOT NULL, -- This refers to the StorageType enum. There are 6 possible values.
    PRIMARY KEY (`datanode_uuid`, `storage_id`),
    FOREIGN KEY (`datanode_uuid`) REFERENCES `datanodes` (`datanode_uuid`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- This table stores intermediate block reports (i.e., blocks received and deleted).
CREATE TABLE `intermediate_block_reports` (
    `report_id` int(11) NOT NULL,
    `datanode_uuid` varchar(36) NOT NULL,
    `pool_id` varchar(255) NOT NULL,
    `received_and_deleted_blocks` varchar(5000) NOT NULL,
    PRIMARY KEY (`report_id`, `datanode_uuid`),
    FOREIGN KEY (`datanode_uuid`) REFERENCES `datanodes` (`datanode_uuid`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `hdfs_block_infos` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `block_index` int(11) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `block_under_construction_state` int(11) DEFAULT NULL,
  `time_stamp` bigint(20) DEFAULT NULL,
  `primary_node_index` int(11) DEFAULT NULL,
  `block_recovery_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_block_lookup_table` (
  `block_id` bigint(20) NOT NULL,
  `inode_id` int(11) NOT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (block_id) */$$

delimiter $$

CREATE TABLE `hdfs_corrupt_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `timestamp` (`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_excess_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_inode_attributes` (
  `inodeId` int(11) NOT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `diskspace` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inodeId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inodeId) */$$


delimiter $$

CREATE TABLE `hdfs_inodes` (
  `partition_id` int(11) NOT NULL,
  `parent_id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(255) NOT NULL DEFAULT '',
  `id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `group_id` int(11) DEFAULT NULL,
  `modification_time` bigint(20) DEFAULT NULL,
  `access_time` bigint(20) DEFAULT NULL,
  `permission` smallint(6) DEFAULT NULL,
  `client_name` varchar(100) DEFAULT NULL,
  `client_machine` varchar(100) DEFAULT NULL,
  `client_node` varchar(100) DEFAULT NULL,
  `generation_stamp` int(11) DEFAULT NULL,
  `header` bigint(20) DEFAULT NULL,
  `symlink` varchar(255) DEFAULT NULL,
  `subtree_lock_owner` bigint(20) DEFAULT NULL,
  `size` bigint(20) NOT NULL DEFAULT '0',
  `quota_enabled` tinyint NOT NULL,
  `meta_enabled` tinyint DEFAULT 0,
  `is_dir` tinyint NOT NULL,
  `under_construction` tinyint NOT NULL,
  `subtree_locked` tinyint DEFAULT NULL,
  `file_stored_in_db` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`partition_id`,`parent_id`,`name`),
  KEY `pidex` (`parent_id`),
  KEY `inode_idx` (`id`),
  KEY `c1` (`parent_id`,`partition_id`),
  KEY `c2` (`partition_id`,`parent_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (partition_id) */  $$

delimiter $$

drop procedure if exists simpleproc$$

delimiter $$

CREATE PROCEDURE simpleproc ()
BEGIN
	DECLARE lc INTEGER;
	DECLARE tc INTEGER;

	SELECT count(LOGFILE_GROUP_NAME) INTO lc FROM INFORMATION_SCHEMA.FILES where LOGFILE_GROUP_NAME="lg_1";
	IF (lc = 0) THEN
	    CREATE LOGFILE GROUP lg_1 ADD UNDOFILE 'undo_log_0.log' INITIAL_SIZE = 128M ENGINE ndbcluster;
	ELSE
		select "The LogFile has already been created" as "";
	END IF;


	SELECT count(TABLESPACE_NAME) INTO tc FROM INFORMATION_SCHEMA.FILES where TABLESPACE_NAME="ts_1";
	IF (tc = 0) THEN
		CREATE TABLESPACE ts_1 ADD datafile 'ts_1_data_file_0.dat' use LOGFILE GROUP lg_1 INITIAL_SIZE = 128M  ENGINE ndbcluster;
	ELSE
		select "The DataFile has already been created" as "";
	END IF;
END$$

delimiter $$

CALL simpleproc$$

delimiter $$

CREATE TABLE IF NOT EXISTS `hdfs_ondisk_small_file_inode_data` (
	  `inode_id` int(11) NOT NULL,
	  `data` VARBINARY(2000) NOT NULL,
	  PRIMARY KEY (`inode_id`)
) /*!50100 TABLESPACE `ts_1` STORAGE DISK */ ENGINE=ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

CREATE TABLE IF NOT EXISTS `hdfs_ondisk_medium_file_inode_data` (
  `inode_id` int(11) NOT NULL,
  `data` VARBINARY(4000) NOT NULL,
  PRIMARY KEY (`inode_id`)
) /*!50100 TABLESPACE `ts_1` STORAGE DISK */ ENGINE=ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

 CREATE TABLE IF NOT EXISTS `hdfs_ondisk_large_file_inode_data` (
  `inode_id` int(11) NOT NULL,
  `dindex`    int(11) NOT NULL,
  `data` VARBINARY(8000) NOT NULL,
  PRIMARY KEY (`inode_id`, `dindex`)
) /*!50100 TABLESPACE `ts_1` STORAGE DISK */ ENGINE=ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

CREATE TABLE `hdfs_inmemory_file_inode_data` (
  `inode_id` int(11) NOT NULL,
  `data` varbinary(1024) NOT NULL,
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */  $$

delimiter $$

CREATE TABLE `hdfs_users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (id) */  $$

delimiter $$

CREATE TABLE `hdfs_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (id) */  $$

delimiter $$

CREATE TABLE `hdfs_users_groups` (
  `user_id` int(11) NOT NULL,
  `group_id` int(11)  NOT NULL,
  PRIMARY KEY (`user_id`, `group_id`),
  CONSTRAINT `user_id`
    FOREIGN KEY (`user_id`)
    REFERENCES `hdfs_users` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION,
 CONSTRAINT `group_id`
    FOREIGN KEY (`group_id`)
    REFERENCES `hdfs_groups` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (user_id) */  $$

delimiter $$

CREATE TABLE `hdfs_invalidated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_le_descriptors` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `rpc_addresses` varchar(128) NOT NULL,
  `http_address` varchar(100) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
PRIMARY KEY (`id`,`partition_val`),
KEY `part` (`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (partition_val) */$$

delimiter $$

CREATE TABLE `yarn_le_descriptors` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `rpc_addresses` varchar(128) NOT NULL,
  `http_address` varchar(100) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`partition_val`),
  KEY `part` (`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (partition_val) */$$

delimiter $$

CREATE TABLE `hdfs_lease_paths` (
  `holder_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  `last_block_id` bigint(20) DEFAULT -1,
  `penultimate_block_id` bigint(20) DEFAULT -1,
  PRIMARY KEY (`holder_id`,`path`),
  KEY `path_idx` (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (holder_id) */$$

delimiter $$

CREATE TABLE `hdfs_leases` (
  `holder_id` int(11) NOT NULL DEFAULT '0',
  `holder` varchar(255) NOT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`holder_id`,`holder`),
  KEY `update_idx` (`last_update`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (holder_id) */$$

delimiter $$

CREATE TABLE `hdfs_misreplicated_range_queue` (
  `range` varchar(120) NOT NULL,
  PRIMARY KEY (`range`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$


delimiter $$

CREATE TABLE `hdfs_path_memcached` (
  `path` varchar(128) NOT NULL,
  `inodeids` varbinary(13500) NOT NULL,
  PRIMARY KEY (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$


delimiter $$

CREATE TABLE `hdfs_pending_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `time_stamp` bigint(20) NOT NULL,
  `num_replicas_in_progress` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_replica_under_constructions` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `state` int(11) DEFAULT NULL,
  `bucket_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `bucket_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`),
  KEY `hash_bucket_idx` (`bucket_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_safe_blocks` (
  `id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$


delimiter $$

CREATE TABLE `hdfs_storage_id_map` (
  `storage_id` varchar(128) NOT NULL,
  `sid` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$


delimiter $$

CREATE TABLE `hdfs_under_replicated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `level` int(11) DEFAULT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`),
  KEY `level` (`level`,`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_variables` (
  `id` int(11) NOT NULL,
  `value` varbinary(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$


delimiter $$

CREATE TABLE `hdfs_quota_update` (
  `id` int(11) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `namespace_delta` bigint(20) DEFAULT NULL,
  `diskspace_delta` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

CREATE TABLE `hdfs_hash_buckets` (
  `storage_id` int(11) NOT NULL,
  `bucket_id` int(11) NOT NULL,
  `hash` bigint NOT NULL,
  PRIMARY KEY (`storage_id`,`bucket_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (storage_id) */$$

delimiter $$

CREATE TABLE `hdfs_encoding_status` (
  `inode_id` int(11) NOT NULL,
  `status` int(11) DEFAULT NULL,
  `codec` varchar(8) DEFAULT NULL,
  `target_replication` smallint(11) DEFAULT NULL,
  `parity_status` int(11) DEFAULT NULL,
  `status_modification_time` bigint(20) DEFAULT NULL,
  `parity_status_modification_time` bigint(20) DEFAULT NULL,
  `parity_inode_id` int(11) DEFAULT NULL,
  `parity_file_name` char(36) DEFAULT NULL,
  `lost_blocks` int(11) DEFAULT 0,
  `lost_parity_blocks` int(11) DEFAULT 0,
  `revoked` tinyint DEFAULT 0,
  PRIMARY KEY (`inode_id`),
  UNIQUE KEY `parity_inode_id` (`parity_inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `hdfs_block_checksum` (
  `inode_id` int(11) NOT NULL,
  `block_index` int(11) NOT NULL,
  `checksum` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

CREATE TABLE `hdfs_on_going_sub_tree_ops` ( 
  `partition_id` int(11) NOT NULL DEFAULT '0',                             
  `path` varchar(3000) NOT NULL,                                           
  `namenode_id` bigint(20) NOT NULL,                                       
  `op_name` int(11) NOT NULL,                                              
  PRIMARY KEY (`partition_id`,`path`),                                     
  KEY `partindex` (`partition_id`),                                        
  KEY `nameidx` (`path`)                                                   
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (partition_id) */$$                              

delimiter $$

CREATE TABLE `hdfs_encoding_jobs` (
  `jt_identifier` varchar(50) NOT NULL,
  `job_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  `job_dir` varchar(200) NOT NULL,
  PRIMARY KEY (`jt_identifier`,`job_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `hdfs_repair_jobs` (
  `jt_identifier` varchar(50) NOT NULL,
  `job_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  `in_dir` varchar(3000) NOT NULL,
  `out_dir` varchar(3000) NOT NULL,
  PRIMARY KEY (`jt_identifier`,`job_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `hdfs_metadata_log` (
  `dataset_id` int(11) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  `inode_partition_id` int(11) NOT NULL,
  `inode_parent_id` int(11) NOT NULL,
  `inode_name` varchar(255) NOT NULL DEFAULT '',
  `operation` smallint(11) NOT NULL,
  PRIMARY KEY (`dataset_id` ,`inode_id` , `timestamp`),
  KEY `timestamp` (`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `hdfs_inode_dataset_lookup` (
  `inode_id` int(11) NOT NULL,
  `dataset_id` int(11) NOT NULL,
  KEY(`dataset_id`),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `yarn_rmnode` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `hostname` VARCHAR(255) NULL,
  `commandport` INT NULL,
  `httpport` INT NULL,
  `healthreport` VARCHAR(500) NULL,
  `lasthealthreporttime` BIGINT NULL,
  `currentstate` VARCHAR(45) NULL,
  `nodemanager_version` VARCHAR(45) NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`))
ENGINE = ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs
PACK_KEYS = DEFAULT PARTITION BY KEY(rmnodeid)$$


delimiter $$

CREATE TABLE `yarn_resource` (
  `id` VARCHAR(255) NOT NULL,
  `memory` INT NULL,
  `virtualcores` INT NULL,
  `gpus` INT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(id)$$

delimiter $$

CREATE TABLE `yarn_updatedcontainerinfo` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  `updatedcontainerinfoid` INT NOT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`, `containerid`, `updatedcontainerinfoid`),
  INDEX `containerid` (`containerid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(`rmnodeid`)$$

delimiter $$

CREATE TABLE `yarn_containerstatus` (
  `containerid` VARCHAR(45) NOT NULL,
  `rmnodeid` VARCHAR(255) NOT NULL,
  `state` VARCHAR(45) NULL,
  `diagnostics` VARCHAR(2000) NULL,
  `exitstatus` INT NULL,
  `uciid` INT NOT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`containerid`, `rmnodeid`, uciid),
  INDEX `rmnodeid_idx` (`rmnodeid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(`rmnodeid`)$$

delimiter $$

CREATE TABLE `yarn_containerid_toclean` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`rmnodeid`, `containerid`),
  INDEX `rmnodeId` (`containerid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(rmnodeid) $$

delimiter $$

CREATE TABLE `yarn_rmnode_applications` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `applicationid` VARCHAR(45) NOT NULL,
  `status` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`rmnodeid`, `applicationid`,`status`),
  INDEX `index2` (`rmnodeid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(applicationid) $$

delimiter $$

CREATE TABLE `yarn_rms_load` (
  `rmhostname` VARCHAR(100) NOT NULL,
  `load` BIGINT NULL,
PRIMARY KEY (`rmhostname`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `yarn_nextheartbeat` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `nextheartbeat` INT NULL,
  PRIMARY KEY (`rmnodeid`)
)ENGINE = ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(rmnodeid)$$

delimiter $$

CREATE TABLE `yarn_pendingevents` (
  `id` INT NOT NULL,
  `rmnodeid` VARCHAR(255) NOT NULL,
  `type`  VARCHAR(255) NULL,
  `status` VARCHAR(255) NULL,
  `contains` INT NULL,
  PRIMARY KEY (`id`, `rmnodeid`))
ENGINE = ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `yarn_applicationstate` (
  `applicationid` VARCHAR(45) NOT NULL,
  `appstate` VARBINARY(13500) NULL,
  `appuser` VARCHAR(45) NULL,
  `appname` VARCHAR(200) NULL,
  `appsmstate` VARCHAR(45) NULL,
PRIMARY KEY (`applicationid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(`applicationid`)$$

delimiter $$

CREATE TABLE `yarn_delegation_token` (
  `seq_number` INT NOT NULL,
  `rmdt_identifier` VARBINARY(13500) NULL,
PRIMARY KEY (`seq_number`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `yarn_delegation_key` (
  `key` INT NOT NULL,
  `delegationkey` VARBINARY(13500) NULL,
PRIMARY KEY (`key`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs$$

delimiter $$

CREATE TABLE `yarn_applicationattemptstate` (
  `applicationid` VARCHAR(45) NOT NULL,
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `applicationattemptstate` VARBINARY(13000) NULL,
  `applicationattempttrakingurl` VARCHAR(120) NULL,
  PRIMARY KEY (`applicationid`, `applicationattemptid`),
  INDEX `applicationid` (`applicationid` ASC),
  CONSTRAINT `applicationid`
  FOREIGN KEY (`applicationid`)
  REFERENCES `yarn_applicationstate` (`applicationid`)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(`applicationid`)$$

delimiter $$

CREATE TABLE `yarn_projects_quota` (
  `projectname` VARCHAR(100) NOT NULL,
  `total` FLOAT DEFAULT '0',
  `quota_remaining` FLOAT  DEFAULT '0',
  PRIMARY KEY (`projectname`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(projectname)$$

delimiter $$

CREATE TABLE `yarn_containers_logs` (
  `container_id` VARCHAR(255) NOT NULL,
  `start` BIGINT NOT NULL,
  `stop` BIGINT  DEFAULT NULL,
  `exit_status` INT DEFAULT NULL,
  `price` FLOAT  DEFAULT NULL,
  `vcores` INT DEFAULT NULL,
  `gpus` INT DEFAULT NULL,
  `mb` INT DEFAULT NULL,
  PRIMARY KEY (`container_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs $$

delimiter $$

CREATE TABLE `yarn_projects_daily_cost` (
  `user` VARCHAR(255) NOT NULL,
  `projectname` VARCHAR(100) NOT NULL,
  `day` BIGINT NOT NULL,
  `credits_used` FLOAT  DEFAULT NULL,
  PRIMARY KEY (`projectname`, `day`, `user`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(user)$$

delimiter $$

CREATE TABLE `yarn_containers_checkpoint` (
  `container_id` VARCHAR(255) NOT NULL,
  `checkpoint` BIGINT NOT NULL,
  `multiplicator` FLOAT NOT NULL,
  PRIMARY KEY (`container_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(container_id)$$

delimiter $$

CREATE TABLE `yarn_price_multiplicator` (
  `id` VARCHAR(255) NOT NULL,
  `multiplicator` FLOAT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs $$

delimiter $$

CREATE TABLE `yarn_container_to_signal` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  `command` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`rmnodeid`, `containerid`),
  INDEX `rmnodeId` (`containerid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(rmnodeid) $$

delimiter $$

CREATE TABLE `yarn_container_to_decrease` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  `http_address` VARCHAR(255) NOT NULL,
  `priority` INT NOT NULL,
  `memory_size` BIGINT NOT NULL,
  `virtual_cores` INT NOT NULL,
  `gpus` INT NOT NULL,
  `version` INT NOT NULL,
  PRIMARY KEY (`rmnodeid`, `containerid`),
  INDEX `rmnodeId` (`containerid` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(rmnodeid) $$

delimiter $$

CREATE TABLE `yarn_reservation_state` (
  `plan_name` VARCHAR(255) NOT NULL,
  `reservation_id_name` VARCHAR(255) NOT NULL,
  `state` VARBINARY(13000) NOT NULL,
  PRIMARY KEY (`plan_name`, `reservation_id_name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(reservation_id_name) $$
