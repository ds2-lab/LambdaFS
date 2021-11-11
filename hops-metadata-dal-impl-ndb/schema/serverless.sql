-- This file contains tables defined for Serverless HopsFS.
-- NOTE: The number of `deployment_invalidation` tables should correspond to the number of deployments you have.
--       If you have 10 deployments, you need 10 of those tables. If you add deployments, you need to create more
--       of those tables. At some point, we may create a script to help automate this process.
--       The same holds true for the `write_acknowledgements` tables.

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
    `published_at` bigint(20) NOT NULL,
    `pool_id` varchar(255) NOT NULL,
    `received_and_deleted_blocks` varchar(5000) NOT NULL,
    PRIMARY KEY (`report_id`, `datanode_uuid`),
    FOREIGN KEY (`datanode_uuid`) REFERENCES `datanodes` (`datanode_uuid`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

---------------------------------
------ Invalidation Tables ------
---------------------------------

CREATE TABLE `invalidations_deployment0` (
    `inode_id` int(11) NOT NULL,        -- The INode's ID.
    `parent_id` int(11) NOT NULL,       -- The INode ID of the parent of the INode being invalidated.
    `leader_id` bigint(20) NOT NULL,    -- The NameNodeID of the NN who issued the invalidation.
    `tx_start` bigint(20) NOT NULL,     -- The time at which the associated transaction began.
    `op_id` bigint(20) NOT NULL,        -- Unique identifier of the associated write operation/transaction.
    PRIMARY KEY(`inode_id`, `leader_id`, `op_id`),
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `invalidations_deployment1` (
    `inode_id` int(11) NOT NULL,        -- The INode's ID.
    `parent_id` int(11) NOT NULL,       -- The INode ID of the parent of the INode being invalidated.
    `leader_id` bigint(20) NOT NULL,    -- The NameNodeID of the NN who issued the invalidation.
    `tx_start` bigint(20) NOT NULL,     -- The time at which the associated transaction began.
    `op_id` bigint(20) NOT NULL,        -- Unique identifier of the associated write operation/transaction.
    PRIMARY KEY(`inode_id`, `leader_id`, `op_id`),
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `invalidations_deployment2` (
    `inode_id` int(11) NOT NULL,        -- The INode's ID.
    `parent_id` int(11) NOT NULL,       -- The INode ID of the parent of the INode being invalidated.
    `leader_id` bigint(20) NOT NULL,    -- The NameNodeID of the NN who issued the invalidation.
    `tx_start` bigint(20) NOT NULL,     -- The time at which the associated transaction began.
    `op_id` bigint(20) NOT NULL,        -- Unique identifier of the associated write operation/transaction.
    PRIMARY KEY(`inode_id`, `leader_id`, `op_id`),
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

----------------------------------
-- Write Acknowledgement Tables --
----------------------------------

CREATE TABLE `write_acks_deployment0` (
    `namenode_id` bigint(20) NOT NULL,                -- The ID of the NameNode object.
    `deployment_number` varchar(36) NOT NULL,         -- The name of the serverless function in which the NN is running.
    `acknowledged` tinyint(4) NOT NULL DEFAULT '0',   -- Flag indicating whether or not the write has been ACK'd.
    `op_id` bigint(20) NOT NULL,                      -- Unique identifier of the write operation.
    `timestamp` bigint(20) NOT NULL,                  -- The time at which this write operation began.
    PRIMARY KEY (`namenode_id`, `op_id`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `write_acks_deployment1` (
    `namenode_id` bigint(20) NOT NULL,                -- The ID of the NameNode object.
    `deployment_number` varchar(36) NOT NULL,         -- The name of the serverless function in which the NN is running.
    `acknowledged` tinyint(4) NOT NULL DEFAULT '0',   -- Flag indicating whether or not the write has been ACK'd.
    `op_id` bigint(20) NOT NULL,                      -- Unique identifier of the write operation.
    `timestamp` bigint(20) NOT NULL,                  -- The time at which this write operation began.
    PRIMARY KEY (`namenode_id`, `op_id`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `write_acks_deployment2` (
    `namenode_id` bigint(20) NOT NULL,                -- The ID of the NameNode object.
    `deployment_number` varchar(36) NOT NULL,         -- The name of the serverless function in which the NN is running.
    `acknowledged` tinyint(4) NOT NULL DEFAULT '0',   -- Flag indicating whether or not the write has been ACK'd.
    `op_id` bigint(20) NOT NULL,                      -- Unique identifier of the write operation.
    `timestamp` bigint(20) NOT NULL,                  -- The time at which this write operation began.
    PRIMARY KEY (`namenode_id`, `op_id`)
) ENGINE=NDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;