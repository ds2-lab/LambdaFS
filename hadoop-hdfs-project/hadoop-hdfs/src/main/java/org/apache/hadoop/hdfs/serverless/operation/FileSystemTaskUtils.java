package org.apache.hadoop.hdfs.serverless.operation;

import com.google.gson.JsonObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.tcpserver.NameNodeTCPClient;
import org.apache.hadoop.hdfs.serverless.tcpserver.RedirectedRequestFuture;

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
     * @param functionUriBase The endpoint we issue HTTP requests to in order to invoke NameNodes.
     * @param tcpResult The result that will eventually be returned to the client.
     * @param serverlessNameNode The ServerlessNameNode instance running in this container.
     *
     * @return A FileSystemTask for the given request, or null if something went wrong while creating the task.
     */
    public static Future<Serializable> createAndEnqueueFileSystemTask(
            String requestId, String op, JsonObject fsArgs, String functionUriBase,
            NameNodeResult tcpResult, ServerlessNameNode serverlessNameNode) throws IOException {
        if (!fsArgs.has("src"))
            throw new IllegalArgumentException("Arguments for operation " + op + " do not contain a 'src' entry.");

        String src = fsArgs.getAsJsonPrimitive("src").getAsString();

        boolean authorized = FileSystemTaskUtils.checkIfAuthorized(op, src, serverlessNameNode);

        if (!authorized) {
            int targetDeployment = serverlessNameNode.getMappedServerlessFunction(src);
            LOG.debug("We are NOT authorized to perform a write operation on target file/directory " + src);
            LOG.debug("Redirecting request to deployment #" + targetDeployment + " instead...");

            // Create an ExecutorService to execute the HTTP and TCP requests concurrently.
            ExecutorService executorService = Executors.newFixedThreadPool(1);

            // Create a CompletionService to listen for results from the futures we're going to create.
            CompletionService<JsonObject> completionService =
                    new ExecutorCompletionService<JsonObject>(executorService);

            // Submit the HTTP request here.
            Future<JsonObject> future = completionService.submit(() ->
                    serverlessNameNode.getServerlessInvoker().redirectRequest(op, functionUriBase, new JsonObject(),
                            fsArgs,  requestId, targetDeployment));

            return new RedirectedRequestFuture(requestId, op, future);
        }

        FileSystemTask<Serializable> newTask = new FileSystemTask<Serializable>(requestId, op, fsArgs);

        // We wait for the task to finish executing in a separate try-catch block so that, if there is
        // an exception, then we can log a specific message indicating where the exception occurred. If we
        // waited for the task in this next block, we wouldn't be able to indicate in the log whether the
        // exception occurred when creating/scheduling the task or while waiting for it to complete.
        try {
            // The task does exist, so let's enqueue it.
            LOG.debug("[TCP] Adding task " + requestId + " (operation = " + op + ") to work queue now...");
            serverlessNameNode.enqueueFileSystemTask(newTask);
        } catch (InterruptedException ex) {
            LOG.error("[TCP] Encountered " + ex.getClass().getSimpleName()
                    + " while assigning a new task to the worker thread: ", ex);
            tcpResult.addException(ex);
            // We don't want to continue as we already encountered a critical error, so just return.
            return null;
        }

        return newTask;
    }

    /**
     * Check if this NameNode is authorized to perform the specified operation. Specifically, we check if we're
     * being asked to perform a write operation on an INode that we do not cache. If this is the case, then we invoke
     * a function from the deployment responsible for this NameNode, and we return the result of that operation
     * to the client.
     *
     * @param op The name of the operation we're performing.
     * @param src The target file/directory of the operation we've been asked to perform.
     * @param serverlessNameNode The ServerlessNameNode instance running in this container.
     *
     * @return True if we're allowed to perform this operation, otherwise false.
     */
    public static boolean checkIfAuthorized(String op, String src, ServerlessNameNode serverlessNameNode)
            throws IOException {
        if (serverlessNameNode.isWriteOperation(op))
            return serverlessNameNode.authorizedToPerformWrite(src);

        return true;
    }
}
