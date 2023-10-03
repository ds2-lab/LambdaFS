alter table yarn_containers_logs MODIFY `mb` BIGINT;
ALTER TABLE hdfs_inodes ADD COLUMN `logical_time` INT(11) NOT NULL DEFAULT '0';
ALTER TABLE hdfs_metadata_log CHANGE COLUMN `timestamp` `logical_time` INT(11) NOT NULL, DROP INDEX `timestamp`, ADD INDEX `logical_time` (`logical_time` ASC);
update yarn_price_multiplicator set id = 'GENERAL' where id = 'VARIABLE';
