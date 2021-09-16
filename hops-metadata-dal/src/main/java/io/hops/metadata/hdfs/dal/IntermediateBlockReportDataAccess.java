package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

import java.util.List;

public interface IntermediateBlockReportDataAccess<T> extends EntityDataAccess {
    /**
     * Find and return the intermediate block report identified by the given reportId and associated with the
     * datanode identified by the given UUID.
     * @param reportId ID of the desired report.
     * @param datanodeUuid The datanode with which the report is associated/originiated from.
     */
    T getReport(int reportId, String datanodeUuid) throws StorageException;

    /**
     * Retrieve all reports associated with the datanode identified by the given UUID.
     */
    List<T> getReports(String datanodeUuid) throws StorageException;

    /**
     * Retrieve all reports associated with the datanode identified by the given UUID and whose reportId is >=
     * the given minimum reportId.
     */
    List<T> getReports(String datanodeUuid, int minimumReportId) throws StorageException;

    /**
     * Add the necessary information to imitate the blockReceivedAndDeleted() RPC call.
     */
    void addReport(int reportId, String datanodeUuid, String poolId, String receivedAndDeletedBlocks)
            throws StorageException;

    /**
     * Delete the Intermediate Block Reports associated with the DataNode identified by the given UUID.
     * @param datanodeUuid The UUID of the DataNode whose Intermediate Block Reports are to be deleted.
     * @return The number of Intermediate Block Reports that were deleted.
     */
    int deleteReports(String datanodeUuid) throws StorageException;
}
