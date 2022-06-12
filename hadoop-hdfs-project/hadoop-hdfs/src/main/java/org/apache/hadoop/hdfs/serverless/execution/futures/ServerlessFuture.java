package org.apache.hadoop.hdfs.serverless.execution.futures;

import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.execution.results.NullResult;
import org.apache.hadoop.hdfs.serverless.userserver.UserServer;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerBase;

import java.util.concurrent.*;

/**
 * Abstract base class for futures. These are returned by the {@link UserServer} and {@link ServerlessInvokerBase}
 * instances (or w/e subclass of {@link ServerlessInvokerBase} is being used) when operations are submitted to NNs.
 *
 * @param <T> The type of the result to be posted to the future. Typically, subclasses of this class will have a
 *           specific type that is used. For example, HTTP requests will use {@link JsonObject}.
 */
public abstract class ServerlessFuture<T> implements Future<T> {
    private static final Log LOG = LogFactory.getLog(ServerlessFuture.class);

    protected enum State {WAITING, DONE, CANCELLED, ERROR}

    protected volatile State state = State.WAITING;

    /**
     * The unique ID identifying this future.
     */
    protected final String requestId;

    /**
     * The name of the operation that will be performed to fulfill this future.
     */
    protected final String operationName;

    /**
     * Return value of System.nanoTime() called in the constructor of this instance.
     */
    protected final long createdAt;

    /**
     * This is used to receive the result of the future from the worker thread.
     */
    protected final BlockingQueue<T> resultQueue = new ArrayBlockingQueue<>(1);

    public ServerlessFuture(String requestId, String operationName) {
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
     */
    public abstract void cancel(String reason, boolean shouldRetry) throws InterruptedException;

    /**
     * Attempts to cancel execution of this task.
     *
     * @param mayInterruptIfRunning true if the thread executing this task should be interrupted; otherwise,
     *                              in-progress tasks are allowed to complete
     * @return false if the task could not be cancelled, often because it's already completed normally; true otherwise.
     */
    public abstract boolean cancel(boolean mayInterruptIfRunning);

    @Override
    public synchronized boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public synchronized boolean isDone() {
        return state == State.DONE || state == State.CANCELLED;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        if (LOG.isDebugEnabled()) LOG.debug("Waiting for result for request " + requestId + " now...");
        final T resultOrNull = this.resultQueue.take();
        if (LOG.isDebugEnabled()) LOG.debug("Got result for future " + requestId + ".");

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        if (resultOrNull instanceof NullResult)
            return null;

        return resultOrNull;
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final T resultOrNull = this.resultQueue.poll(timeout, unit);
        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        if (resultOrNull instanceof NullResult)
            return null;

        return resultOrNull;
    }

    /**
     * Post a result to this future so that it may be consumed by whoever is waiting on it.
     * This is how the TCP server returns results to the client that issued a file system operation.
     * The client should be blocked, waiting for the result to be placed into the resultQueue by the server.
     *
     * @return True if we were able to insert the result into the queue, otherwise false.
     */
    public boolean postResultImmediate(T result) {
        try {
            boolean success = resultQueue.offer(result);

            if (success)
                this.state = State.DONE;
            else
                LOG.error("Could not post result for future " + getRequestId() + " as result queue is full.");

            return success;
        }
        catch (Exception ex) {
            LOG.error("Exception encountered while attempting to post result to TCP future: ", ex);
            this.state = State.ERROR;
        }

        return false;
    }

    /**
     * Post a result to this future so that it may be consumed by whoever is waiting on it.
     */
    public void postResult(T result) {
        try {
            resultQueue.put(result);
            this.state = State.DONE;
        }
        catch (Exception ex) {
            LOG.error("Exception encountered while attempting to post result to TCP future: ", ex);
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

        if (!(obj instanceof ServerlessFuture))
            return false;

        ServerlessFuture other = (ServerlessFuture)obj;

        return this.requestId.equals(other.requestId);
    }
}
