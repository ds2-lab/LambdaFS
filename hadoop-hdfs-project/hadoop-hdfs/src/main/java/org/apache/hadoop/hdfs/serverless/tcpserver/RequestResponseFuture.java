package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryo.util.Null;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.FileSystemTask;
import org.apache.hadoop.hdfs.serverless.operation.NameNodeWorkerThread;
import org.apache.hadoop.hdfs.serverless.operation.NullResult;

import java.io.Serializable;
import java.util.concurrent.*;

/**
 * These are created when issuing TCP requests to NameNodes. They are registered with the TCP Server so that
 * the server can return results for particular operations back to the client waiting on the result.
 *
 * These are used on the client side.
 */
public class RequestResponseFuture implements Future<JsonObject> {
    private static final Log LOG = LogFactory.getLog(FileSystemTask.class);

    private enum State {WAITING, DONE, CANCELLED, ERROR}

    private volatile State state = State.WAITING;

    /**
     * The unique ID identifying this future.
     */
    private final String requestId;

    /**
     * The name of the operation that will be performed to fulfill this future.
     */
    private final String operationName;

    /**
     * Return value of System.nanoTime() called in the constructor of this instance.
     */
    private final long createdAt;

    /**
     * This is used to receive the result of the future from the worker thread.
     */
    private final BlockingQueue<Object> resultQueue = new ArrayBlockingQueue<>(1);

    public RequestResponseFuture(String requestId, String operationName) {
        this.requestId = requestId;
        this.operationName = operationName;
        this.createdAt = System.nanoTime();
    }

    /**
     * Cancel this future, informing whoever is waiting on it why it was cancelled and if we think
     * they should retry (via HTTP this time, seeing as the TCP connection was presumably lost).
     *
     * TODO: Add way to check if the future was really cancelled.
     *
     * @param reason The reason for cancellation.
     * @param shouldRetry If True, then whoever is waiting on this future should resubmit.
     *
     * @throws InterruptedException
     */
    public synchronized void cancel(String reason, boolean shouldRetry) throws InterruptedException {
        state = State.CANCELLED;
        JsonObject cancellationMessage = new JsonObject();
        cancellationMessage.addProperty(ServerlessNameNodeKeys.REQUEST_ID, requestId);
        cancellationMessage.addProperty(ServerlessNameNodeKeys.OPERATION, operationName);
        cancellationMessage.addProperty(ServerlessNameNodeKeys.CANCELLED, true);
        cancellationMessage.addProperty(ServerlessNameNodeKeys.REASON, reason);
        cancellationMessage.addProperty(ServerlessNameNodeKeys.SHOULD_RETRY, shouldRetry);
        resultQueue.put(cancellationMessage);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        LOG.warn("Standard cancel API is not supported.");
        return false;
    }

    @Override
    public synchronized boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public synchronized boolean isDone() {
        return state == State.DONE || state == State.CANCELLED;
    }

    @Override
    public synchronized JsonObject get() throws InterruptedException, ExecutionException {
        final Object resultOrNull = this.resultQueue.take();
        LOG.debug("Got result for future " + requestId + ".");

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        if (resultOrNull instanceof NullResult)
            return null;
        else if (resultOrNull instanceof JsonObject)
            return (JsonObject)resultOrNull;
        else
            throw new IllegalArgumentException("Received invalid object type as response for request " + requestId
                    + ". Object type: " + resultOrNull.getClass().getSimpleName());
    }

    @Override
    public synchronized JsonObject get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final Object resultOrNull = this.resultQueue.poll(timeout, unit);
        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        if (resultOrNull instanceof NullResult)
            return null;
        else if (resultOrNull instanceof JsonObject)
            return (JsonObject)resultOrNull;
        else
            throw new IllegalArgumentException("Received invalid object type as response for request " + requestId
                    + ". Object type: " + resultOrNull.getClass().getSimpleName());
    }

    /**
     * Post a result to this future so that it may be consumed by whoever is waiting on it.
     */
    public synchronized void postResult(JsonObject result) {
        LOG.debug("Posting result for future " + requestId + " now...");
        try {
            resultQueue.put(result);
            LOG.debug("Posted result for future " + requestId + ".");
            this.state = State.DONE;
        }
        catch (Exception ex) {
            LOG.error("Exception encountered while attempting to put result into future: ", ex);
            this.state = State.ERROR;
        }
    }

    /**
     * Returns the amount of time (in milliseconds) that has elapsed since this task was created.
     * @return the amount of time, in milliseconds, that has elapsed since this task was created.
     */
    public long getTimeElapsedSinceCreation() {
        long timeElapsedNano = System.nanoTime() - createdAt;

        return TimeUnit.NANOSECONDS.toMillis(timeElapsedNano);
    }

    public long getCreatedAt() { return createdAt; }

    public String getOperationName() {
        return operationName;
    }

    public String getRequestId() {
        return requestId;
    }

    /**
     * Hash based on requestId.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((requestId.hashCode()));
        return result;
    }

    /**
     * Equality based on requestId.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof RequestResponseFuture))
            return false;

        RequestResponseFuture other = (RequestResponseFuture)obj;

        return this.requestId.equals(other.requestId);
    }
}
