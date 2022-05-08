package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

public class ZooKeeperInvalidation implements Serializable {
    private static final long serialVersionUID = 940708969191512359L;
    /**
     * The unique ID of the NN orchestrating the consistency protocol and associated transaction/write operation.
     */
    private long leaderNameNodeId;

    /**
     * Unique ID of the write operation/transaction associated with this invalidation.
     */
    private long operationId;

    /**
     * If true, then this is treated as a subtree invalidation. Otherwise, this is treated as a normal invalidation.
     */
    private boolean subtreeInvalidation;

    /**
     * The root of the subtree being invalidated, assuming this is a subtree invalidation.
     */
    private String subtreeRoot;

    /**
     * All the INodes (their IDs, specifically) that are being affected by the associated write operation.
     */
    private List<Long> invalidatedINodes;

    public ZooKeeperInvalidation(long leaderNameNodeId, long operationId, List<Long> invalidatedINodes,
                                 boolean subtreeInvalidation, String subtreeRoot) {
        this.leaderNameNodeId = leaderNameNodeId;
        this.operationId = operationId;
        this.subtreeInvalidation = subtreeInvalidation;
        this.invalidatedINodes = invalidatedINodes;
        this.subtreeRoot = subtreeRoot;
    }

    public long getLeaderNameNodeId() {
        return leaderNameNodeId;
    }

    public void setLeaderNameNodeId(long leaderNameNodeId) {
        this.leaderNameNodeId = leaderNameNodeId;
    }

    public long getOperationId() {
        return operationId;
    }

    public void setOperationId(long operationId) {
        this.operationId = operationId;
    }

    @Override
    public String toString() {
        return "ZooKeeperInvalidation(Leader NN ID=" + leaderNameNodeId + ", Operation ID=" + operationId +
                ", Subtree INV = " + subtreeInvalidation + (subtreeInvalidation ? ", Subtree Root=" + subtreeRoot +
                ")" : ", Invalidated INodes: " + StringUtils.join(invalidatedINodes, ", ") + ")");
    }

    public boolean isSubtreeInvalidation() {
        return subtreeInvalidation;
    }

    public void setSubtreeInvalidation(boolean subtreeInvalidation) {
        this.subtreeInvalidation = subtreeInvalidation;
    }

    public List<Long> getInvalidatedINodes() {
        return invalidatedINodes;
    }

    public void setInvalidatedINodes(List<Long> invalidatedINodes) {
        this.invalidatedINodes = invalidatedINodes;
    }

    public String getSubtreeRoot() {
        return subtreeRoot;
    }

    public void setSubtreeRoot(String subtreeRoot) {
        this.subtreeRoot = subtreeRoot;
    }
}
