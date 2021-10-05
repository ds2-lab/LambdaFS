package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.core.store.EventOperation;

public class HopsEventOperation {
    /**
     * We need to maintain an instance to this class in order to perform the necessary operations.
     */
    private EventOperation clusterJEventOperation;

    public HopsEventOperation(EventOperation clusterJEventOperation) {
        this.clusterJEventOperation = clusterJEventOperation;
    }

    /**
     * Return the underlying/wrapped ClusterJ EventOperation. This is package private as the ClusterJ EventOperation
     * object should only be interfaced with by this library. It is abstracted away for clients of this library.
     * @return the underlying/wrapped ClusterJ EventOperation.
     */
    protected EventOperation getClusterJEventOperation() {
        return clusterJEventOperation;
    }
}
