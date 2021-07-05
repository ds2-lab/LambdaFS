package io.hops.metadata.hdfs.entity;

/**
 * POJO encapsulating a StorageReport. Used in conjunction with NDB.
 */
public class StorageReport {
    /**
     * We can tell which reports are grouped together by this field.
     */
    private final int groupId;

    /**
     * This is how we distinguish between entire groups of storage reports.
     */
    private final int reportId;

    private final boolean failed;

    private final long dfsUsed;

    private final long remaining;

    private final long blockPoolUsed;

    private final String datanodeStorageId;

    public StorageReport(int groupId, int reportId, boolean failed, long dfsUsed,
                         long remaining, long blockPoolUsed, String datanodeStorageId) {
        this.groupId = groupId;
        this.reportId = reportId;
        this.failed = failed;
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

    public boolean isFailed() {
        return failed;
    }

    public int getReportId() {
        return reportId;
    }

    public int getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return "StorageReport < groupId = " + groupId + ", reportId = " + reportId + ", failed = " + failed +
                ", dfsUsed = " + dfsUsed + ", remaining = " + remaining + ", blockPoolUsed = " + blockPoolUsed +
                ", datanodeStorageId = " + datanodeStorageId + ">";
    }
}
