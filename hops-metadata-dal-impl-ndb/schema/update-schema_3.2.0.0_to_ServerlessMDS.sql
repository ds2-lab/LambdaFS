-- Indicates whether or not that INode is about to be written to.
ALTER TABLE `hdfs_inodes` ADD COLUMN `INV` tinyint(4) NOT NULL DEFAULT '0';