package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

import java.util.List;

/**
 * Interface defining the API used to read and write StorageReport objects to NDB.
 */
public interface StorageReportDataAccess<T> extends EntityDataAccess {
    /**
     * Retrieve the particular report identified by the given groupId and reportId.
     * @param groupId The ID of the group of storageReports in which the desired instance is found.
     * @param reportId The ID of the particular report.
     */
    T getStorageReport(int groupId, int reportId, String datanodeUuid) throws StorageException;

    /**
     * Remove the particular storage report identified by the given groupId and reportId.
     * @param groupId The ID of the group of storageReports in which the desired instance is found.
     * @param reportId The ID of the particular report.
     */
    void removeStorageReport(int groupId, int reportId, String datanodeUuid) throws StorageException;

    /**
     * Remove all StorageReport instances contained within the group specified by the given groupId.
     * @param groupId The ID of the group of StorageReport instances to remove.
     * @throws StorageException
     */
    void removeStorageReports(int groupId, String datanodeUuid) throws StorageException;

    /**
     * Store the given StorageReport instance in NDB.
     * @param storageReport The instance to store in NDB.
     */
    void addStorageReport(T storageReport) throws StorageException;

    /**
     * Retrieve all StorageReport instances contained within the group specified by the given groupId.
     * @param groupId The ID of the group of StorageReports to return.
     * @param datanodeUuid The UUID of the datanode from which the reports should have originated.
     */
    List<T> getStorageReports(int groupId, String datanodeUuid) throws StorageException;

    /**
     * Retrieve the latest storage reports from the DataNode identified by the given UUID.
     *
     * @param datanodeUuid The UUID of the datanode from which the reports should have originated.
     */
    List<T> getLatestStorageReports(String datanodeUuid) throws StorageException;
}
