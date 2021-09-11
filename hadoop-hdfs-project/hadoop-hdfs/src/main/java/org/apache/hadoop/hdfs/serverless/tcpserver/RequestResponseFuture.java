package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.google.gson.JsonObject;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.operation.FileSystemTask;
import org.apache.hadoop.hdfs.serverless.operation.NameNodeWorkerThread;

import java.io.Serializable;
import java.util.concurrent.*;

/**
 * These are created when issuing TCP requests to NameNodes. They are registered with the TCP Server so that
 * the server can return results for particular operations back to the client waiting on the result.
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

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        state = State.CANCELLED;
        return true;
    }

    @Override
    public synchronized boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public synchronized boolean isDone() {
        return state == State.DONE;
    }

    @Override
    public synchronized JsonObject get() throws InterruptedException, ExecutionException {
        final Object resultOrNull = this.resultQueue.take();

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        if (resultOrNull == NullRequestResponse.getInstance())
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

        if (resultOrNull == NullRequestResponse.getInstance())
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
    public void postResult(JsonObject result) {
        try {
            resultQueue.put(result);
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
     * Used to return 'null' for a TCP request. If this is the result that is obtained, then
     * null is returned in place of an actual object (i.e., something that implements Serializable).
     */
    public static class NullRequestResponse implements Serializable {
        private static final long serialVersionUID = -1663521618378958991L;

        static NameNodeWorkerThread.NullResult instance;

        public static NameNodeWorkerThread.NullResult getInstance() {
            if (instance == null)
                instance = new NameNodeWorkerThread.NullResult();

            return instance;
        }
    }
}
