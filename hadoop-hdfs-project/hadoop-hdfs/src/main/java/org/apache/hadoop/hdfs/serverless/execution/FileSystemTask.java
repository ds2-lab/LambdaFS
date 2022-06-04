package org.apache.hadoop.hdfs.serverless.execution;

import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.concurrent.*;

/**
 * Encapsulates a user-specified File System operation to be executed by the NameNode.
 *
 * This is used to avoid duplicate requests from being processed when clients issue concurrent TCP and HTTP requests.
 *
 * This is used on the NameNode side.
 */
public class FileSystemTask<T extends Serializable> implements Future<T> {
    private static final Log LOG = LogFactory.getLog(FileSystemTask.class);

    /**
     * Return the flag indicating whether the task should be re-executed,
     * even if it was already completed once.
     */
    public boolean getForceRedo() {
        return forceRedo;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    private enum State {WAITING, DONE, CANCELLED, ERROR}

    private volatile State state = State.WAITING;

    /**
     * The unique ID identifying this future.
     */
    private final String taskId;

    /**
     * The name of the operation that will be performed to fulfill this future.
     */
    private final String operationName;

    /**
     * The arguments to be passed to the associated file system operation.
     */
    private final JsonObject operationArguments;

    /**
     * Return value of System.nanoTime() called in the constructor of this instance.
     */
    private final long createdAt;

    /**
     * Indicates whether this task was submitted via HTTP or TCP.
     */
    private final String requestMethod;

    /**
     * This is used to receive the result of the future from the worker thread.
     */
    private final BlockingQueue<T> resultQueue = new ArrayBlockingQueue<>(1);

    /**
     * If True, the NN should complete this operation again, even if it was already completed once before.
     * This can occur if the TCP connection that was going to return the result back to the client was
     * dropped before the client received the result.
     */
    private final boolean forceRedo;

    /**
     * Create a new instance of FileSystemTask.
     *
     * @param taskId The unique ID of this task. This ID comes from the requestId and is shared by the HTTP and
     *               TCP requests used to submit this action.
     * @param operationName The name of the file system operation being performed. This generally refers explicitly
     *                      to the function name of the operation.
     * @param operationArguments The arguments for the file system operation.
     * @param forceRedo If True, the NN should complete this operation again, even if it was already completed
     *                  once before. Presumably the TCP connection that was going to return the result back to
     *                  the client was dropped before the client received the result.
     * @param requestMethod Indicates whether this task was submitted via HTTP or TCP.
     */
    public FileSystemTask(String taskId, String operationName, JsonObject operationArguments,
                          boolean forceRedo, String requestMethod) {
        this.taskId = taskId;
        this.operationName = operationName;
        this.operationArguments = operationArguments;
        this.forceRedo = forceRedo;
        this.requestMethod = requestMethod;

        this.createdAt = System.nanoTime();
    }

    /**
     * Create a new instance of FileSystemTask.
     *
     * @param taskId The unique ID of this task. This ID comes from the requestId and is shared by the HTTP and
     *               TCP requests used to submit this action.
     * @param operationName The name of the file system operation being performed. This generally refers explicitly
     *                      to the function name of the operation.
     * @param operationArguments The arguments for the file system operation.
     * @param requestMethod Indicates whether this task was submitted via HTTP or TCP.
     */
    public FileSystemTask(String taskId, String operationName, JsonObject operationArguments, String requestMethod) {
        this(taskId, operationName, operationArguments, false, requestMethod);
    }

    @Override
    public String toString() {
        return "FileSystemTask(taskId=" + taskId + ", op=" + operationName + ",requestMethod=" + requestMethod +
                ",createdAt=" + createdAt + ", state=" + state.name() + ", forceRedo=" + forceRedo + ")";
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        return false;
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
    public synchronized T get() throws InterruptedException, ExecutionException {
        final T resultOrNull = this.resultQueue.take();

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        //if (resultOrNull instanceof NullResult || resultOrNull instanceof DuplicateRequest)
        //    return null;

        return resultOrNull;
    }

    @Override
    public synchronized T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {

        final T resultOrNull = this.resultQueue.poll(timeout, unit);

        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        // Check if the NullResult object was placed in the queue, in which case we should return null.
        //if ((resultOrNull instanceof NullResult || resultOrNull instanceof DuplicateRequest))
        //    return null;

        return resultOrNull;
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

    public JsonObject getOperationArguments() {
        return operationArguments;
    }

    public String getOperationName() {
        return operationName;
    }

    public String getTaskId() {
        return taskId;
    }
}
