package io.hops.metadata.hdfs.entity;

public class IntermediateBlockReport {
    /**
     * Used as one part of the primary key.
     */
    private final int reportId;

    /**
     * Used as one part of the primary key.
     *
     * Identifies the DataNode associated with this report.
     */
    private final String datanodeUuid;

    /**
     * The timestamp at which the report was published.
     */
    private final long publishedAt;

    private final String poolId;

    /**
     * Base64-encoded object of type StorageReceivedDeletedBlocks[].
     */
    private final String receivedAndDeletedBlocks;

    public IntermediateBlockReport(int reportId, String datanodeUuid, long publishedAt,
                                   String poolId, String receivedAndDeletedBlocks) {
        this.reportId = reportId;
        this.datanodeUuid = datanodeUuid;
        this.publishedAt = publishedAt;
        this.poolId = poolId;
        this.receivedAndDeletedBlocks = receivedAndDeletedBlocks;
    }

    public int getReportId() {
        return reportId;
    }

    public String getDatanodeUuid() {
        return datanodeUuid;
    }

    public String getPoolId() {
        return poolId;
    }

    public String getReceivedAndDeletedBlocks() {
        return receivedAndDeletedBlocks;
    }

    @Override
    public String toString() {
        return "IntermediateBlockReport <reportId = " + reportId + ", datanodeUuid = " + datanodeUuid + ", poolId = "
                + poolId + ">";
    }

    public long getPublishedAt() {
        return publishedAt;
    }
}
