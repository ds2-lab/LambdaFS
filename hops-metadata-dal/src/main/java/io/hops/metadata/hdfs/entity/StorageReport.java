package io.hops.metadata.hdfs.entity;

/**
 * POJO encapsulating a StorageReport. Used in conjunction with NDB.
 */
public class StorageReport {
    /**
     * We can tell which reports are grouped together by this field.
     */
    private final long groupId;

    /**
     * This is how we distinguish between entire groups of storage reports.
     */
    private final int reportId;

    /**
     * This refers to the unique UUID of the Datanode from which this report originated.
     */
    private final String datanodeUuid;

    private final boolean failed;

    private final long capacity;

    private final long dfsUsed;

    private final long remaining;

    private final long blockPoolUsed;

    private final String datanodeStorageId;

    public StorageReport(long groupId, int reportId, String datanodeUuid, boolean failed, long capacity,
                         long dfsUsed, long remaining, long blockPoolUsed, String datanodeStorageId) {
        this.groupId = groupId;
        this.reportId = reportId;
        this.datanodeUuid = datanodeUuid;
        this.failed = failed;
        this.capacity = capacity;
        this.dfsUsed = dfsUsed;
        this.remaining = remaining;
        this.blockPoolUsed = blockPoolUsed;
        this.datanodeStorageId = datanodeStorageId;
    }

    public String getDatanodeStorageId() {
        return datanodeStorageId;
    }

    public long getBlockPoolUsed() {
        return blockPoolUsed;
    }

    public long getRemaining() {
        return remaining;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public boolean getFailed() {
        return failed;
    }

    public int getReportId() {
        return reportId;
    }

    public long getGroupId() {
        return groupId;
    }

    public long getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return "StorageReport < groupId = " + groupId + ", reportId = " + reportId + ", datanodeUuid = " + datanodeUuid +
                ", failed = " + failed + ", capacity = " + capacity + ", dfsUsed = " + dfsUsed +
                ", remaining = " + remaining + ", blockPoolUsed = " + blockPoolUsed +
                ", datanodeStorageId = " + datanodeStorageId + ">";
    }

    public String getDatanodeUuid() {
        return datanodeUuid;
    }
}
