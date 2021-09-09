package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServerlessNameNodeTask<T extends Serializable> implements Future<T> {
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
     * The final result obtained by executing the specified operation.
     */
    private Serializable taskResult;

    /**
     * Flag indicating whether this future is cancelled.
     */
    private boolean cancelled = false;

    public ServerlessNameNodeTask(String taskId, String operationName, JsonObject operationArguments) {
        this.taskId = taskId;
        this.operationName = operationName;
        this.operationArguments = operationArguments;
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        this.cancelled = true;

        return false;
    }

    @Override
    public synchronized boolean isCancelled() {
        return cancelled;
    }

    @Override
    public synchronized boolean isDone() {
        return taskResult != null;
    }

    @Override
    public synchronized T get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public synchronized T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

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
