package io.hops.metadata.hdfs.entity;

/**
 * Defines INode cache invalidation used by serverless NameNodes during transactions/subtree operations.
 */
public class Invalidation {
    /**
     * INode ID.
     */
    private final long inodeId;

    /**
     * Parent INode ID.
     */
    private final long parentId;

    /**
     * Unique NameNode ID of the "Leader NameNode"; that is, the NameNode that
     * issued the invalidation. They're the one performing the write operation.
     */
    private final long leaderNameNodeId;

    /**
     * The time at which the associated
     */
    private final long txStartTime;

    /**
     * The unique ID of the write operation/transaction.
     */
    private final long operationId;

    public Invalidation(long inodeId, long parentId, long leaderNameNodeId, long txStartTime, long operationId) {
        this.inodeId = inodeId;
        this.parentId = parentId;
        this.leaderNameNodeId = leaderNameNodeId;
        this.txStartTime = txStartTime;
        this.operationId = operationId;
    }

    @Override
    public String toString() {
        return "Invalidation (INodeID=" + inodeId + ", ParentINodeID=" + parentId + ", leaderNameNodeID=" +
                leaderNameNodeId + ", txStartTime=" + txStartTime + ", operationID=" + operationId + ")";
    }

    public long getOperationId() {
        return operationId;
    }

    public long getTxStartTime() {
        return txStartTime;
    }

    public long getLeaderNameNodeId() {
        return leaderNameNodeId;
    }

    public long getParentId() {
        return parentId;
    }

    public long getINodeId() {
        return inodeId;
    }
}
