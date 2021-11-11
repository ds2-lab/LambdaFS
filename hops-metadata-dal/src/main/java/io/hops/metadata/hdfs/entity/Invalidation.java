package io.hops.metadata.hdfs.entity;

/**
 * Defines INode cache invalidation used by serverless NameNodes during transactions/subtree operations.
 */
public class Invalidation {
    /**
     * INode ID.
     */
    private final int inodeId;

    /**
     * Parent INode ID.
     */
    private final int parentId;

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

    public Invalidation(int inodeId, int parentId, long leaderNameNodeId, long txStartTime, long operationId) {
        this.inodeId = inodeId;
        this.parentId = parentId;
        this.leaderNameNodeId = leaderNameNodeId;
        this.txStartTime = txStartTime;
        this.operationId = operationId;
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

    public int getParentId() {
        return parentId;
    }

    public int getINodeId() {
        return inodeId;
    }
}
