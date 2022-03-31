package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.google.gson.JsonObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.*;

/**
 * Some functions commonly used by TCP and HTTP handlers when creating file system tasks.
 */
public class FileSystemTaskUtils {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(FileSystemTaskUtils.class);

    /**
     * Create and return a FileSystemTask instance for the given request. This function checks to see if this
     * NameNode is write-authorized to perform the task. If not, this function transparently redirects the request
     * to the correct deployment.
     *
     * Important:
     * If this NameNode is executing the task locally (i.e., it was not redirected), then this function enqueues
     * the task in the NameNode's work queue. On the other hand, if the task was redirected, then we do NOT enqueue
     * it. We simply return the future, and the client calls .get() on that future.
     *
     * TODO: Add retry/timeout handling for redirected future.
     *       This might be done most easily by creating a new class for redirected futures where the retry logic
     *       is abstracted away from the client who simply calls .get() on the future...
     *
     * @param requestId The ID of the task/request.
     * @param op The name of the operation we're performing.
     * @param fsArgs The file system arguments supplied by the client.
     * @param tcpResult The result that will eventually be returned to the client.
     * @param serverlessNameNode The ServerlessNameNode instance running in this container.
     * @param requestMethod Indicates whether this task was submitted via HTTP or TCP.
     * @return A FileSystemTask for the given request, or null if something went wrong while creating the task.
     */
    public static Future<Serializable> createAndEnqueueFileSystemTask(
            String requestId, String op, JsonObject fsArgs, NameNodeResult tcpResult,
            ServerlessNameNode serverlessNameNode, boolean forceRedo, String requestMethod) {

        FileSystemTask<Serializable> newTask = new FileSystemTask<>(requestId, op, fsArgs, forceRedo, requestMethod);

        // We wait for the task to finish executing in a separate try-catch block so that, if there is
        // an exception, then we can log a specific message indicating where the exception occurred. If we
        // waited for the task in this next block, we wouldn't be able to indicate in the log whether the
        // exception occurred when creating/scheduling the task or while waiting for it to complete.
        try {
            // The task does exist, so let's enqueue it.
            if (LOG.isDebugEnabled()) LOG.debug("Adding task " + requestId + " (operation = " + op + ") to work queue now...");
            serverlessNameNode.enqueueFileSystemTask(newTask);
        } catch (InterruptedException ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName()
                    + " while assigning a new task to the worker thread: ", ex);
            tcpResult.addException(ex);
            // We don't want to continue as we already encountered a critical error, so just return.
            return null;
        }

        return newTask;
    }
}
