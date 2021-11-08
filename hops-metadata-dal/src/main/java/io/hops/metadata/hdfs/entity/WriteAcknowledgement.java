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
    private final boolean acknowledged;

    /**
     * The time at which the associated write operation began.
     */
    private final long timestamp;

    public WriteAcknowledgement(long nameNodeId, int deploymentNumber, long operationId,
                                boolean acknowledged, long timestamp) {
        this.nameNodeId = nameNodeId;
        this.deploymentNumber = deploymentNumber;
        this.operationId = operationId;
        this.acknowledged = acknowledged;
        this.timestamp = timestamp;
    }

    /**
     * Return an instance of this WriteAcknowledgement object with the 'acknowledged' field set to True.
     */
    public WriteAcknowledgement acknowledge() {
        return new WriteAcknowledgement(this.nameNodeId, this.deploymentNumber, this.operationId,
                true, this.timestamp);
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
                "operationId=" + operationId + ", acknowledged=" + acknowledged + ", timestamp=" + timestamp;
    }
}
