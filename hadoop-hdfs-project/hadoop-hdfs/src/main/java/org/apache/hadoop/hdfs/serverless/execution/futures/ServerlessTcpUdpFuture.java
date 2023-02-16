package org.apache.hadoop.hdfs.serverless.execution.futures;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.execution.results.CancelledResult;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.execution.results.NullResult;
import org.apache.hadoop.hdfs.serverless.userserver.TcpUdpRequestPayload;
import org.apache.hadoop.hdfs.serverless.userserver.UserServer;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * These are created when issuing TCP/UDP requests to NameNodes. They are registered with the {@link UserServer}
 * instance so that the server can return results for particular operations back to the client waiting on the result.
 *
 * These are used on the client side.
 */
public class ServerlessTcpUdpFuture extends ServerlessFuture<NameNodeResult> {
    private static final Log LOG = LogFactory.getLog(ServerlessTcpUdpFuture.class);

    /**
     * The NameNodeID of the NN this request was sent to.
     */
    private final long targetNameNodeId;

    public ServerlessTcpUdpFuture(TcpUdpRequestPayload associatedPayload, long targetNameNodeId) {
        super(associatedPayload.getRequestId(), associatedPayload.getOperationName());

        /**
         * The payload that was submitted for this request.
         */
        this.targetNameNodeId = targetNameNodeId;
    }

    @Override
    public NameNodeResult get() throws InterruptedException, ExecutionException {
        if (LOG.isDebugEnabled()) LOG.debug("Waiting for result for request " + requestId + " now...");
        final NameNodeResult resultOrNull = this.resultQueue.take();
        if (LOG.isDebugEnabled()) LOG.debug("Got result for future " + requestId + ".");

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        // Note: This presently cannot happen, as we always make the type parameter
        //       a NameNodeResult, and NullResult is a completely separate class.
        if (resultOrNull instanceof NullResult)
            return null;

        return resultOrNull;
    }

    @Override
    public NameNodeResult get(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final NameNodeResult resultOrNull = this.resultQueue.poll(timeout, unit);
        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        // Note: This presently cannot happen, as we always make the type parameter
        //       a NameNodeResult, and NullResult is a completely separate class.
        if (resultOrNull instanceof NullResult)
            return null;

        return resultOrNull;
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
        resultQueue.put(CancelledResult.getInstance());
        if (LOG.isDebugEnabled()) LOG.debug("Cancelled future " + requestId + " for operation " +
                operationName + ". Reason: " + reason);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new NotImplementedException("cancel(boolean) is not supported for TCP/UDP futures.");
    }
}
