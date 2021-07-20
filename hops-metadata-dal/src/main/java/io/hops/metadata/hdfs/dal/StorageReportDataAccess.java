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
     * Remove all StorageReport instances associated with the datanode identified by the given UUID.
     */
    void removeStorageReports(String datanodeUuid) throws StorageException;

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

    /**
     * Get and return the largest value of groupId associated with the DataNode identified by the given UUID.
     */
    int getLastGroupId(String datanodeUuid) throws StorageException;

    /**
     * Retrieve all StorageReport instances with a groupId strictly greater than the parameterized groupId.
     *
     * The StorageReport instances retrieved will all be associated with the DataNode identified by the given uuid.
     * @param groupId The groupId of the last successfully-retrieved report. All reports with a strictly larger groupId
     *                will be retrieved during this method's execution.
     * @param datanodeUuid The DataNode whose storage reports the caller is interested in.
     * @return A list of StorageReport instances.
     * @throws StorageException
     */
    List<T> getStorageReportsAfterGroupId(int groupId, String datanodeUuid) throws StorageException;
}
