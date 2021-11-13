package io.hops.metadata.hdfs.entity;

public class WriteAcknowledgement {
    /**
     * The unique ID of the NameNode who needs to ACK.
     */
    private final long nameNodeId;

    /**
     * The full deployment name of the functions involved in the write operation.
     */
    private final int deploymentNumber;

    /**
     * This is the unique ID of the write operation, so it can be distinguished from other write operations.
     */
    private final long operationId;

    /**
     * Indicates whether this particular WriteAcknowledgement entry has been acknowledged or not.
     */
    private boolean acknowledged;

    /**
     * The time at which the associated write operation began.
     */
    private final long timestamp;

    /**
     * The ID of the NameNode who created this write ack entry in the first place.
     * They're the one performing the associated write operation/transaction.
     */
    private final long leaderNameNodeId;

    public WriteAcknowledgement(long nameNodeId, int deploymentNumber, long operationId,
                                boolean acknowledged, long timestamp, long leaderNameNodeId) {
        this.nameNodeId = nameNodeId;
        this.deploymentNumber = deploymentNumber;
        this.operationId = operationId;
        this.acknowledged = acknowledged;
        this.timestamp = timestamp;
        this.leaderNameNodeId = leaderNameNodeId;
    }

    /**
     * Return an instance of this WriteAcknowledgement object with the 'acknowledged' field set to True.
     */
    public void acknowledge() {
        this.acknowledged = true;
    }

    public long getNameNodeId() {
        return nameNodeId;
    }

    public int getDeploymentNumber() {
        return deploymentNumber;
    }

    public long getOperationId() {
        return operationId;
    }

    public boolean getAcknowledged() {
        return acknowledged;
    }

    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "WriteAcknowledgement(nameNodeId=" + nameNodeId + ", deploymentNumber=" + deploymentNumber +
                ", operationId=" + operationId + ", acknowledged=" + acknowledged + ", timestamp=" + timestamp +
                ", leaderId=" + leaderNameNodeId + ")";
    }

    public long getLeaderNameNodeId() {
        return leaderNameNodeId;
    }
}
