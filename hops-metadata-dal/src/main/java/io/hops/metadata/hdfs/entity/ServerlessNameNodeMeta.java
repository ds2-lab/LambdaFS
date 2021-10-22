package io.hops.metadata.hdfs.entity;

/**
 * POJO encapsulating a Serverless NameNode (specifically its metadata) as stored in the intermediate storage.
 */
public final class ServerlessNameNodeMeta {
    /**
     * The ID of the NameNode object.
     */
    private final long nameNodeId;

    /**
     * The name of the serverless function.
     */
    private final String functionName;

    /**
     * Basically a place-holder for the future if we scale-out deployments.
     */
    private final String replicaId;

    /**
     * Time at which this NameNode instance started running.
     */
    private final long creationTime;

    public ServerlessNameNodeMeta(long nameNodeId, String functionName, String replicaId, long creationTime) {
        this.nameNodeId = nameNodeId;
        this.functionName = functionName;
        this.replicaId = replicaId;
        this.creationTime = creationTime;
    }

    public long getNameNodeId() {
        return nameNodeId;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getReplicaId() {
        return replicaId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return "ServerlessNameNodeMeta(nameNodeId=" + nameNodeId + ", functionName=" + functionName +
                ", replicaId=" + replicaId + ", creationTime=" + creationTime + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof ServerlessNameNodeMeta))
            return false;

        ServerlessNameNodeMeta other = (ServerlessNameNodeMeta)obj;

        return this.nameNodeId == other.nameNodeId &&
                this.functionName.equals(other.functionName);
                // Eventually might include: this.replicaId.equals(other.replicaId)
    }

    /**
     * Mimics the compareTo() function in some sense. If the creation times are the same, this returns 0.
     *
     * If the creation time of THIS instance is less, then this returns -1. If it is greater, then it returns 1.
     *
     * In that sense, the ordering imposed by this function is NOT consistent with equals().
     * @param other The other NN against which we are comparing.
     * @return 0 if both DNs were created at the same time, -1 if THIS instance was created earlier, 1 if THIS
     * instance was created later.
     */
    public int compareCreationTimes(ServerlessNameNodeMeta other) {
        return Long.compare(this.creationTime, other.creationTime);
    }
}
