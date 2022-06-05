package org.apache.hadoop.hdfs.serverless.execution.futures;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.execution.results.CancelledResult;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.userserver.TcpUdpRequestPayload;
import org.apache.hadoop.hdfs.serverless.userserver.UserServer;

/**
 * These are created when issuing TCP/UDP requests to NameNodes. They are registered with the {@link UserServer}
 * instance so that the server can return results for particular operations back to the client waiting on the result.
 *
 * These are used on the client side.
 */
public class ServerlessTcpUdpFuture extends ServerlessFuture<NameNodeResult> {
    private static final Log LOG = LogFactory.getLog(ServerlessTcpUdpFuture.class);

    /**
     * The payload that was submitted for this request.
     */
    private final TcpUdpRequestPayload associatedPayload;

    /**
     * The NameNodeID of the NN this request was sent to.
     */
    private final long targetNameNodeId;

    public ServerlessTcpUdpFuture(TcpUdpRequestPayload associatedPayload, long targetNameNodeId) {
        super(associatedPayload.getRequestId(), associatedPayload.getOperationName());

        this.associatedPayload = associatedPayload;
        this.targetNameNodeId = targetNameNodeId;
    }

    public long getTargetNameNodeId() {
        return this.targetNameNodeId;
    }

    /**
     * Cancel this future, informing whoever is waiting on it why it was cancelled and if we think
     * they should retry (via HTTP this time, seeing as the TCP connection was presumably lost).
     *
     * TODO: Add way to check if the future was really cancelled.
     *
     * @param reason The reason for cancellation.
     * @param shouldRetry If True, then whoever is waiting on this future should resubmit.
     */
    @Override
    public void cancel(String reason, boolean shouldRetry) throws InterruptedException {
        state = State.CANCELLED;
        // associatedPayload.setCancelled(true);
        // associatedPayload.setShouldRetry(shouldRetry);
        // associatedPayload.setCancellationReason(reason);
        resultQueue.put(CancelledResult.instance);
        if (LOG.isDebugEnabled()) LOG.debug("Cancelled future " + requestId + " for operation " +
                operationName + ". Reason: " + reason);
    }
}
