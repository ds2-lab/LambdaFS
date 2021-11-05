package io.hops.metadata.hdfs.entity;

public class WriteAcknowledgement {
    /**
     * The unique ID of the NameNode who needs to ACK.
     */
    private final long nameNodeId;

    /**
     * The full deployment name of the functions involved in the write operation.
     *
     * TODO: Would it be better if this was JUST the deployment number?
     */
    private final String functionName;

    /**
     * This is the unique ID of the write operation, so it can be distinguished from other write operations.
     */
    private final String operationId;

    /**
     * Indicates whether this particular WriteAcknowledgement entry has been acknowledged or not.
     */
    private final boolean acknowledged;

    public WriteAcknowledgement(long nameNodeId, String functionName, String operationId, boolean acknowledged) {
        this.nameNodeId = nameNodeId;
        this.functionName = functionName;
        this.operationId = operationId;
        this.acknowledged = acknowledged;
    }

    /**
     * Return an instance of this WriteAcknowledgement object with the 'acknowledged' field set to True.
     */
    public WriteAcknowledgement acknowledge() {
        return new WriteAcknowledgement(this.nameNodeId, this.functionName, this.operationId, true);
    }

    public long getNameNodeId() {
        return nameNodeId;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getOperationId() {
        return operationId;
    }

    public boolean getAcknowledged() {
        return acknowledged;
    }

    @Override
    public String toString() {
        return "WriteAcknowledgement(nameNodeId=" + nameNodeId + ", functionName=" + functionName +
                "operationId=" + operationId + ", acknowledged=" + acknowledged;
    }
}
