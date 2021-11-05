package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.google.gson.JsonObject;
import org.apache.avro.data.Json;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerBase;
import org.apache.hadoop.hdfs.serverless.operation.NullResult;

import java.io.Serializable;
import java.util.concurrent.*;

public class RedirectedRequestFuture implements Future<Serializable> {
    private static final Log LOG = LogFactory.getLog(RedirectedRequestFuture.class);

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
     * We wrap this future, which is returned by issuing an HTTP request to a NameNode.
     */
    private final Future<JsonObject> requestFuture;

    public RedirectedRequestFuture(String requestId, String operationName, Future<JsonObject> requestFuture) {
        this.requestId = requestId;
        this.operationName = operationName;
        this.createdAt = System.nanoTime();
        this.requestFuture = requestFuture;
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
    public Serializable get() throws InterruptedException, ExecutionException {
        LOG.debug("Waiting for result for TCP request " + requestId + " now...");
        final JsonObject resultOrNull = requestFuture.get();
        LOG.debug("Got result for TCP future " + requestId + ".");

        if (resultOrNull != null)
            return extractResult(resultOrNull);
        else
            throw new IllegalArgumentException("Received null result from redirected request " + requestId +
                    ", op = " + operationName + ".");
    }

    @Override
    public Serializable get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        LOG.debug("Waiting for result for TCP request " + requestId + " now...");
        final JsonObject resultOrNull = requestFuture.get(timeout, unit);
        LOG.debug("Got result for TCP future " + requestId + ".");
        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        return extractResult(resultOrNull);
    }

    /**
     * Returns the amount of time (in milliseconds) that has elapsed since this task was created.
     * @return the amount of time, in milliseconds, that has elapsed since this task was created.
     */
    public long getTimeElapsedSinceCreation() {
        long timeElapsedNano = System.nanoTime() - createdAt;

        return TimeUnit.NANOSECONDS.toMillis(timeElapsedNano);
    }

    private Serializable extractResult(JsonObject responseJson) {
        return (Serializable)ServerlessInvokerBase.extractResultFromJsonResponse(responseJson, null);
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

        if (!(obj instanceof RedirectedRequestFuture))
            return false;

        RedirectedRequestFuture other = (RedirectedRequestFuture)obj;

        return this.requestId.equals(other.requestId);
    }
}
