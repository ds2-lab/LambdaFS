package org.apache.hadoop.hdfs.serverless.zookeeper;

import java.io.Serializable;

public class ZooKeeperInvalidation implements Serializable {
    private static final long serialVersionUID = 940708969191512359L;

    private long inodeId;
    private long parentId;
    private long leaderNameNodeId;
    private long operationId;

    public ZooKeeperInvalidation(long inodeId, long parentId, long leaderNameNodeId, long operationId) {
        this.inodeId = inodeId;
        this.parentId = parentId;
        this.leaderNameNodeId = leaderNameNodeId;
        this.operationId = operationId;
    }

    public long getInodeId() {
        return inodeId;
    }

    public void setInodeId(long inodeId) {
        this.inodeId = inodeId;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
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
        return "ZooKeeperInvalidation(INode ID=" + inodeId + ", Parent INode ID=" + parentId + ", Leader NN ID=" +
                leaderNameNodeId + ", Operation ID=" + operationId + ")";
    }
}
