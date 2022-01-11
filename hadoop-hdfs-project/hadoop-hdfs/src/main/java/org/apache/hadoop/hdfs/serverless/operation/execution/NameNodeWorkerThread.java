package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.esotericsoftware.kryonet.Client;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.transaction.context.TransactionsStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * This thread actually executes file system operations. Tasks (i.e., file system operations wrapped in a Future
 * interface) are added to a work queue. This thread consumes those tasks and returns results to whomever is
 * waiting on them (one of the HTTP or TCP request handlers).
 *
 * This is used on the NameNode side.
 */
public class NameNodeWorkerThread extends Thread {
    public static final Logger LOG = LoggerFactory.getLogger(NameNodeWorkerThread.class.getName());

    /**
     * The ID of the request currently being processed by the worker thread.
     *
     * This assumes the worker thread only executes one task at a time.
     */
    private volatile String requestCurrentlyProcessing;

    /**
     * The ID of the NameNode that this thread is running in.
     */
    private final long nameNodeId;

    private final ServerlessNameNode serverlessNameNodeInstance;

    private final ExecutionManager executionManager;

    /**
     * Time in milliseconds that we poll for tasks before giving up and performing routine activities.
     */
    private final long pollTimeMilliseconds = 60000;

    /**
     * ID number of this worker thread.
     */
    private final int workerThreadId;

    public NameNodeWorkerThread(ServerlessNameNode serverlessNameNodeInstance, ExecutionManager executionManager,
                                int workerThreadId) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.nameNodeId = serverlessNameNodeInstance.getId();
        this.executionManager = executionManager;
        this.workerThreadId = workerThreadId;
    }

    /**
     * Return the ID of the request currently being processed. This will be null if there is no request
     * currently being processed.
     */
    public String getRequestCurrentlyProcessing() {
        return requestCurrentlyProcessing;
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread #" + workerThreadId + " has started running.");

        FileSystemTask<Serializable> task = null;
        while(true) {
            try {
                task = executionManager.getWork();

                LOG.debug("Worker thread " + workerThreadId + ": dequeued task " + task.getTaskId() + "(operation = "
                                + task.getOperationName() + ").");

                // This will ultimately be returned to the main thread to be merged with their NameNodeResult instance.
                // The requestId and requestMethod fields won't be used during the merge, so the values we use here
                // for the task/request ID and the operation name won't necessarily persist back to the client. (In
                // general, they should be the same as whatever the main thread has, so they WILL persist.)
                NameNodeResult workerResult = new NameNodeResult(this.serverlessNameNodeInstance.getDeploymentNumber(),
                        task.getTaskId(), task.getOperationName(), this.nameNodeId);
                workerResult.setDequeuedTime(Time.getUtcTime());

                if (task.getForceRedo())
                    LOG.debug("Task " + task.getTaskId() + " is being resubmitted (force_redo is TRUE).");
                else
                    LOG.debug("Task " + task.getTaskId() + " does NOT appear to be a duplicate.");

                // Clear and reset statistics from previously-executed tasks.
                serverlessNameNodeInstance.getNamesystem().getMetadataCache().clearCurrentRequestCacheCounters();
                serverlessNameNodeInstance.clearTransactionEvents();
                TransactionsStats.getInstance().clearForServerless();
                requestCurrentlyProcessing = task.getTaskId();

                // Execute the request/operation.
                Serializable result = null;
                boolean success = false;
                try {
                    result = serverlessNameNodeInstance.performOperation(
                            task.getOperationName(), task.getOperationArguments());

                    if (result == null)
                        workerResult.addResult(NullResult.getInstance(), true);
                    else
                        workerResult.addResult(result, true);

                    workerResult.setProcessingFinishedTime(Time.getUtcTime());

                    // Commit the statistics to this result.
                    // This stores the statistics in the result so that they will be returned to the client.
                    workerResult.commitStatisticsPackages();
                    workerResult.commitTransactionEvents(serverlessNameNodeInstance.getTransactionEvents());
                    success = true;
                } catch (Exception ex) {
                    LOG.error("Worker thread " + workerThreadId + " encountered exception while executing file system operation " +
                            task.getOperationName() + " for task " + task.getTaskId() + ".", ex);
                    workerResult.addException(ex);
                } catch (Throwable t) {
                    LOG.error("Worker thread " + workerThreadId + " encountered throwable while executing file system operation " +
                            task.getOperationName() + " for task " + task.getTaskId() + ".", t);
                    workerResult.addThrowable(t);
                }

                // If this task was submitted via HTTP, then attempt to create a deployment mapping.
                if (task.getRequestMethod().equalsIgnoreCase("HTTP")) {
                    try {
                        LOG.debug("Trying to create function mapping for request " + task.getTaskId() + " now...");
                        long start = Time.getUtcTime();
                        // Check if a function mapping should be created and returned to the client.
                        OpenWhiskHandler.tryCreateDeploymentMapping(workerResult, task.getOperationArguments(), serverlessNameNodeInstance);
                        long end = Time.getUtcTime();

                        LOG.debug("Created function mapping for request " + task.getTaskId() + " in " + (end - start) + " ms.");
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while trying to create function mapping for task " +
                                task.getTaskId() + ":", ex);
                        workerResult.addException(ex);
                    }
                }

                requestCurrentlyProcessing = null;

                // Post the result to the future. This will wake up the TCP client that is waiting on us.
                // The TCP client will then send the result back to the HopsFS client.
                task.postResult(workerResult);

                // We only add the task to the `completedTasks` mapping if we executed it successfully.
                // If there was an error, then may be automatically re-submitted by the client.
                if (success)
                    executionManager.notifyTaskCompleted(task);

                // Cache the result for a bit.
                PreviousResult previousResult = new PreviousResult(result, task.getOperationName(), task.getTaskId());
                executionManager.cachePreviousResult(task.getTaskId(), previousResult);
            }
            catch (IOException ex) {
                LOG.error("Serverless NameNode Worker Thread " + workerThreadId + " was interrupted while running:", ex);
            }
        }
    }
}
