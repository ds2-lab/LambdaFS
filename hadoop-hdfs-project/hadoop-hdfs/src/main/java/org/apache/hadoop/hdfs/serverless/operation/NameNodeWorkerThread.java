package org.apache.hadoop.hdfs.serverless.operation;

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
     * Reference to the Serverless NameNode instance created in the OpenWhiskHandler class.
     */
    private final ServerlessNameNode serverlessNameNodeInstance;

    /**
     * HTTP and TCP requests will add work to this queue.
     */
    private final BlockingQueue<FileSystemTask<Serializable>> workQueue;

    /**
     * All tasks that are currently being executed. For now, we only ever execute one task at a time.
     */
    private final ConcurrentHashMap<String, FileSystemTask<Serializable>> currentlyExecutingTasks;

    /**
     * All tasks that have been executed by this worker thread.
     */
    private final ConcurrentHashMap<String, FileSystemTask<Serializable>> completedTasks;

    /**
     * The ID of the request currently being processed by the worker thread.
     *
     * This assumes the worker thread only executes one task at a time.
     */
    private volatile String requestCurrentlyProcessing;

    /**
     * How often the worker thread should attempt to purge old records.
     */
    private final long purgeIntervalMilliseconds;

    /**
     * How long to keep a result in the cache before purging it.
     */
    private final long resultRetainIntervalMilliseconds;

    /**
     * Maximum size of the previous results cache.
     */
    private final int previousResultCacheMaxSize;

    /**
     * How often, in seconds, the active name nodes list should be refreshed.
     */
    private final int activeNameNodeRefreshIntervalMilliseconds;

    /**
     * The time at which the active name node list was last refreshed. Used to schedule future refreshes.
     */
    private long lastActiveNameNodeListRefresh;

    /**
     * Cache of previously-computed results. These results are kept in-memory for a configurable period of time
     * so that they may be re-submitted to a client if the client does not receive the original transmission.
     *
     * The NameNode periodically removes a given PreviousResult from its cache after a configurable amount of time
     * has passed.
     *
     * This particular variable is used to order the previous results so that the worker thread can
     * quickly determine which results should be removed from memory.
     */
    // private final PriorityQueue<PreviousResult> previousResultPriorityQueue;

    /**
     * Cache of previously-computed results. These results are kept in-memory for a configurable period of time
     * so that they may be re-submitted to a client if the client does not receive the original transmission.
     *
     * The NameNode periodically removes a given PreviousResult from its cache after a configurable amount of time
     * has passed.
     *
     * This particular variable is used to quickly retrieve a previous result by requestId/taskId.
     */
    private final Cache<String, PreviousResult> previousResultCache;

    /**
     * Mapping from taskId/requestId to the time at which the task was removed from the cache.
     * Just used for debugging.
     */
    private final HashMap<String, Long> purgeRecords;

    /**
     * Time in milliseconds that we poll for tasks before giving up and performing routine activities.
     */
    private final long pollTimeMilliseconds = 5000;

    /**
     * The ID of the NameNode that this thread is running in.
     */
    private final long nameNodeId;

    /**
     * Timestamp of the last time we performed the "routine activities". This is in milliseconds.
     */
    private long lastRoutineActivitiesTime;

    /**
     * The last time that the worker thread attempted to purge old results.
     */
    private long lastPurgePass;

    public NameNodeWorkerThread(
            Configuration conf,
            BlockingQueue<FileSystemTask<Serializable>> workQueue,
            ServerlessNameNode serverlessNameNodeInstance,
            long nameNodeId) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.purgeIntervalMilliseconds = conf.getInt(SERVERLESS_PURGE_INTERVAL_MILLISECONDS,
                SERVERLESS_PURGE_INTERVAL_MILLISECONDS_DEFAULT);
        this.resultRetainIntervalMilliseconds = conf.getInt(SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS,
                SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS_DEFAULT);
        this.previousResultCacheMaxSize = conf.getInt(SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE,
                SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE_DEFAULT);
        this.activeNameNodeRefreshIntervalMilliseconds = conf.getInt(SERVERLESS_ACTIVE_NODE_REFRESH,
                SERVERLESS_ACTIVE_NODE_REFRESH_DEFAULT);

        this.workQueue = workQueue;
        // this.previousResultPriorityQueue = new PriorityQueue<PreviousResult>();
        this.previousResultCache = Caffeine.newBuilder()
                .maximumSize(previousResultCacheMaxSize)
                .initialCapacity((int)(previousResultCacheMaxSize * 0.5))
                .expireAfterWrite(Duration.ofMillis(resultRetainIntervalMilliseconds))
                .expireAfterAccess(Duration.ofMillis(resultRetainIntervalMilliseconds))
                .build();
        this.purgeRecords = new HashMap<String, Long>();
        this.nameNodeId = nameNodeId;

        this.currentlyExecutingTasks = new ConcurrentHashMap<>();
        this.completedTasks = new ConcurrentHashMap<>();
    }

    /**
     * Return the ID of the request currently being processed. This will be null if there is no request
     * currently being processed.
     */
    public String getRequestCurrentlyProcessing() {
        return requestCurrentlyProcessing;
    }

    /**
     * Check if a purging of cached PreviousResult objects should be performed. If so, then perform the purge.
     *
     * This function DOES update the 'lastPurgePass' variable if a purge is performed.
     *
     * @return The number of records that were purged, if a purge was performed. If no purge was performed, then
     * -1 is returned.
     */
//    private int tryPurge() {
//        long now = Time.getUtcTime();
//
//        if (now - lastPurgePass > purgeIntervalMilliseconds) {
//            int numPurged = doPurge();
//            lastPurgePass = Time.getUtcTime();
//            return numPurged;
//        }
//
//        return -1;
//    }

    /**
     * Check if it is time to attempt to update the active NameNode list.
     * If so, then perform the refresh.
     * If not, then return immediately.
     */
    private void tryUpdateActiveNameNodeList() {
        long now = Time.getUtcTime();

        if (now - lastActiveNameNodeListRefresh > activeNameNodeRefreshIntervalMilliseconds) {

            try {
                // Update the list of active name nodes, if it is time to do so.
                serverlessNameNodeInstance.refreshActiveNameNodesList();
            } catch (Exception ex) {
                LOG.error("Encountered Exception while trying to refresh the list of active NameNodes.");
                ex.printStackTrace();
            }

            lastActiveNameNodeListRefresh = Time.getUtcTime();
        }
    }

    /**
     * Try to retrieve and process updates from intermediate storage.
     * These updates include storage reports and intermediate block reports.
     *
     * This was originally performed exclusively in the HTTP handler
     * {@link org.apache.hadoop.hdfs.serverless.OpenWhiskHandler}. We moved it here because, when clients were issuing
     * TCP requests to NameNodes rather than HTTP requests, we would encounter errors as the NNs never retrieved and
     * processed storage reports and intermediate block reports. Now the NameNodeWorkerThread periodically retrieves
     * and processes these updates. Seeing as both HTTP and TCP requests go thru the NameNodeWorkerThread, this
     * prevents the aforementioned issue from occurring.
     *
     * NOTE: The NameNode keeps track of scheduling these updates. So this function will just return
     * if it is not time for an update. The worker thread does not need to check if it is time to
     * update or not itself.
     */
    private void tryUpdateFromIntermediateStorage() {
        try {
            serverlessNameNodeInstance.getAndProcessUpdatesFromIntermediateStorage();
        }
        catch (IOException | ClassNotFoundException ex) {
            LOG.error("Encountered exception while retrieving and processing updates from intermediate storage: ", ex);
        }
    }

    /**
     * Perform the various book-keeping tasks that the worker thread does, such as updating
     * the active name node list for the NameNode, and purging entries from the cache of previous results.
     */
    private void doRoutineActivities() {
        // Check if we need to purge any results before continuing to the next loop.
        // tryPurge();

        // COMMENTED OUT:
        // We use a persistent watcher to automatically get notified of changes in group membership.
        // tryUpdateActiveNameNodeList();
        tryUpdateFromIntermediateStorage();

        this.lastRoutineActivitiesTime = Time.getUtcTime();
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread has started running.");

        doRoutineActivities();

        // Pretend we checked our (currently-empty) cache for results to purge.
        lastPurgePass = Time.getUtcTime();

        FileSystemTask<Serializable> task = null;
        while(true) {
            try {
                task = workQueue.poll(pollTimeMilliseconds, TimeUnit.MILLISECONDS);

                if (task == null) {
                    doRoutineActivities();
                    continue;
                }

                LOG.debug("Worker thread: dequeued task " + task.getTaskId() + "(operation = "
                                + task.getOperationName() + ").");

                // This will ultimately be returned to the main thread to be merged with their NameNodeResult instance.
                // The requestId and requestMethod fields won't be used during the merge, so the values we use here
                // for the task/request ID and the operation name won't necessarily persist back to the client. (In
                // general, they should be the same as whatever the main thread has, so they WILL persist.)
                NameNodeResult workerResult = new NameNodeResult(this.serverlessNameNodeInstance.getDeploymentNumber(),
                        task.getTaskId(), task.getOperationName(), this.nameNodeId);
                workerResult.setDequeuedTime(Time.getUtcTime());

                // Check if this is a duplicate task.
                if (isTaskDuplicate(task) && !task.getForceRedo()) {
                    handleDuplicateTask(task, workerResult);
                    doRoutineActivities();
                    continue;
                }

                if (task.getForceRedo())
                    LOG.debug("Task " + task.getTaskId() + " is being resubmitted (force_redo is TRUE).");
                else
                    LOG.debug("Task " + task.getTaskId() + " does NOT appear to be a duplicate.");

                serverlessNameNodeInstance.getNamesystem().getMetadataCache().clearCurrentRequestCacheCounters();
                serverlessNameNodeInstance.clearTransactionEvents();
                TransactionsStats.getInstance().clearForServerless();
                FileSystemTask<Serializable> prev = currentlyExecutingTasks.putIfAbsent(task.getTaskId(), task);
                requestCurrentlyProcessing = task.getTaskId();
                if (prev != null)
                    LOG.error("Tried to enqueue task " + task.getTaskId() + " into CurrentlyExecutingTasks despite...");

                Serializable result = null;
                boolean success = false;
                try {
                    result = serverlessNameNodeInstance.performOperation(
                            task.getOperationName(), task.getOperationArguments());

                    if (result == null)
                        workerResult.addResult(NullResult.getInstance(), true);
                    else
                        workerResult.addResult(result, true);

                    // Commit the statistics to this result.
                    workerResult.commitStatisticsPackages();
                    workerResult.commitTransactionEvents(serverlessNameNodeInstance.getTransactionEvents());
                    success = true;
                } catch (Exception ex) {
                    LOG.error("Worker thread encountered exception while executing file system operation " +
                            task.getOperationName() + " for task " + task.getTaskId() + ".", ex);
                    workerResult.addException(ex);
                } catch (Throwable t) {
                    LOG.error("Worker thread encountered throwable while executing file system operation " +
                            task.getOperationName() + " for task " + task.getTaskId() + ".", t);
                    workerResult.addThrowable(t);
                }

                // If this task was submitted via HTTP, then attempt to create a deployment mapping.
                if (task.getRequestMethod().equalsIgnoreCase("HTTP")) {
                    try {
                        LOG.debug("Trying to create function mapping for request " + task.getTaskId() + " now...");
                        // Check if a function mapping should be created and returned to the client.
                        OpenWhiskHandler.tryCreateDeploymentMapping(workerResult, task.getOperationArguments(), serverlessNameNodeInstance);
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while trying to create function mapping for task " +
                                task.getTaskId() + ":", ex);
                        workerResult.addException(ex);
                    }
                }

                currentlyExecutingTasks.remove(task.getTaskId());
                requestCurrentlyProcessing = null;

                task.postResult(workerResult);

                // We only add the task to the `completedTasks` mapping if we executed it successfully.
                // If there was an error, then may be automatically re-submitted by the client.
                if (success) {
                    prev = completedTasks.putIfAbsent(task.getTaskId(), task);
                    if (prev != null && !task.getForceRedo())
                        LOG.error("Enqueued task " + task.getTaskId() + " into CompletedTasks even though (a) there " +
                                "is already a task with the same ID present, and (b) we did not redo this task...");
                }

                // Cache the result for a bit.
                LOG.debug("Caching result of task " + task.getTaskId() + "(op=" + task.getOperationName()
                                + ") for " + resultRetainIntervalMilliseconds + " milliseconds.");
                PreviousResult previousResult = new PreviousResult(result, task.getOperationName(), task.getTaskId());
                previousResultCache.put(task.getTaskId(), previousResult);
                // previousResultPriorityQueue.add(previousResult);

                long millisecondsSinceLastRoutineActivities = Time.getUtcTime() - lastRoutineActivitiesTime;
                if (millisecondsSinceLastRoutineActivities >= pollTimeMilliseconds) {
                    LOG.debug("Have not performed routine activities in " + millisecondsSinceLastRoutineActivities +
                            " milliseconds. Performing now. Size of task queue: " + workQueue.size() + ".");
                    doRoutineActivities();
                }
            }
            catch (InterruptedException | IOException ex) {
                LOG.error("Serverless NameNode Worker Thread was interrupted while running:", ex);
            }
        }
    }

//    /**
//     * Iterate over the priority queue, purging previous results until the first result that
//     * is not ready for purging is encountered.
//     *
//     * This function does NOT update the 'lastPurgePass' variable.
//     *
//     * @return The number of previous results that were removed from the cache.
//     */
//    public int doPurge() {
////        LOG.debug("Purging previously-calculated results from the cache now. Size of cache pre-purge is: " +
////                previousResultCache.size());
//        long now = Time.getUtcTime();
//        int numPurged = 0;
//
//        while (previousResultPriorityQueue.size() > 0) {
//            PreviousResult result = previousResultPriorityQueue.peek();
//
//            // Has it been long enough that we should remove this result?
//            if (now - result.timestamp > resultRetainIntervalMilliseconds) {
//                previousResultPriorityQueue.poll();             // Remove it from the priority queue.
//                previousResultCache.remove(result.requestId);   // Remove it from the cache itself.
//                purgeRecords.put(result.requestId, now);        // Make note of when we purged it.
//                // LOG.debug("Purging previous result for task " + result.requestId + " from result cache.");
//                numPurged++;
//                continue;
//            }
//
//            // Since the structure is a priority queue (implemented with a min-heap), older results (with smaller
//            // timestamps) will appear first. If we encounter a result that is not ready to be removed, then
//            // everything after it in the priority queue will also not be ready to be removed, so we should return;
//            break;
//        }
//
////        LOG.debug("Removed " + numPurged + (numPurged == 1 ? " entry " : " entries ") +
////                "from the cache. Size of cache is now " + previousResultCache.size());
//        return numPurged;
//    }

    /**
     * Handler for when the worker thread encounters a duplicate task.
     * @param task The task in question.
     * @param result The NameNodeResult object being used by the worker thread.
     */
    private void handleDuplicateTask(FileSystemTask<Serializable> task, NameNodeResult result) {
        LOG.warn("Encountered duplicate task " + task.getTaskId() + " (operation = "
                + task.getOperationName() + ").");

        if (task.getForceRedo()) {
            LOG.warn("Client did not receive the original transmission of the result for "
                    + task.getTaskId() + ". Checking to see if result is cached...");

            PreviousResult previousResult = previousResultCache.getIfPresent(task.getTaskId());
            if (previousResult != null) {
                LOG.debug("Result for task " + task.getTaskId()
                        + " is still cached. Returning it to the client now.");
                task.postResult(previousResult.value);
            } else {
                LOG.warn("Result for task " + task.getTaskId() + " is no longer in the cache.");
                double timeSinceRemovalSeconds =
                        (Time.getUtcTime() - purgeRecords.get(task.getTaskId())) / 1000.0;
                LOG.warn("Task was removed " + timeSinceRemovalSeconds + " seconds ago.");

                result.addResult(new DuplicateRequest("TCP", task.getTaskId()), true);

                // Simply post a DuplicateRequest message. The client-side code will know that this means
                // that the result is no longer in the cache, and the operation must be restarted.
                task.postResult(result);
            }
        } else {
            result.addResult(new DuplicateRequest("TCP", task.getTaskId()), true);
            task.postResult(result);
        }
    }

    /**
     * Returns the number of tasks currently being executed by the worker thread.
     *
     * Generally speaking, this should just be one.
     */
    public synchronized int numTasksCurrentlyExecuting() {
        return currentlyExecutingTasks.size();
    }

    /**
     * Returns the number of tasks that have been executed by this worker thread.
     */
    public synchronized int numTasksExecuted() {
        return completedTasks.size();
    }

    /**
     * Check if this task is a duplicate based on its task key. The task key comes from the request ID, which
     * is generated client-side. This means that, for a given HTTP request, its corresponding TCP request will have
     * the same request ID (and vice versa).
     *
     * @param candidate the task for which we are checking if it is a duplicate
     * @return true if the task is a duplicate, otherwise false.
     */
    public synchronized boolean isTaskDuplicate(FileSystemTask<Serializable> candidate) {
        return isTaskDuplicate(candidate.getTaskId());
    }

    /**
     * Check if the task identified by the given ID is a duplicate.
     *
     * @param taskId the task ID of the task for which we are checking if it is a duplicate
     * @return true if the task is a duplicate, otherwise false.
     */
    public boolean isTaskDuplicate(String taskId) {
        return currentlyExecutingTasks.containsKey(taskId) || completedTasks.containsKey(taskId);
    }

    /**
     * Get the duplicate task of the given task.
     *
     * For example, if this function is passed a task corresponding to an HTTP request with ID "FOO", then this will
     * return the TCP request with ID "FOO".
     */
    public synchronized FileSystemTask<Serializable> getDuplicateTask(FileSystemTask<Serializable> candidate) {
        if (currentlyExecutingTasks.containsKey(candidate.getTaskId()))
            return currentlyExecutingTasks.get(candidate.getTaskId());

        return completedTasks.get(candidate.getTaskId());
    }

    /**
     * Used to wrap a previously-computed result by the worker thread.
     *
     * The worker thread holds onto previously-computed results for a configurable length of time before
     * discarding them. The results are held onto in case the client does not receive the result. Clients may
     * re-submit the FS operation in this scenario, in which case the worker thread will check if it still has
     * the result in-memory. If it does, it will return it. If it doesn't, then it will return nothing, and the
     * client must simply restart the operation from scratch.
     *
     * For now, the worker thread would not re-perform the given operation was FS operations are obviously not
     * ephemeral and redoing the same operation more than once could have undesirable side effects in some cases.
     *
     * Note: this class has a natural ordering that is inconsistent with equals.
     */
    public static class PreviousResult implements Comparable<PreviousResult> {
        /**
         * The value that was returned by the operation.
         */
        public final Serializable value;

        /**
         * The time that the result was posted back to the main thread by the worker thread.
         */
        public long timestamp;

        /**
         * The name of the operation that was performed to generate this result.
         */
        public final String operationName;

        /**
         * The unique identifier of the original TCP/HTTP request(s) that submitted the operation that generated
         * this result.
         */
        public final String requestId;

        /**
         * Create a new instance of PreviousResult.
         *
         * @param value The result of a particular file system operation.
         * @param timestamp The time at which this result was created.
         */
        public PreviousResult(Serializable value, String operationName, String requestId, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
            this.operationName = operationName;
            this.requestId = requestId;
        }

        /**
         * Create a new instance of PreviousResult. This constructor populates the timestamp field automatically.
         * @param value The result of a particular file system operation.
         */
        public PreviousResult(Serializable value, String operationName, String requestId) {
            this(value, operationName, requestId, Time.getUtcTime());
        }

        /**
         * Get the time elapsed, in milliseconds, since this PreviousResult instance was created.
         */
        public long getTimeElapsedMilliseconds() {
            long now = Time.getUtcTime();
            return now - timestamp;
        }

        /**
         * This should be called if this result is re-submitted back to the client for one reason or another.
         * This will cause the worker thread to retain this result in-memory for longer. Basically, it just resets
         * the counter on when this result will be purged from memory.
         */
        public void updateTimestamp() {
            this.timestamp = Time.getUtcTime();
        }

        @Override
        public String toString() {
            return "PreviousResult (value=" + value.toString() + ", timestamp=" + timestamp + ")";
        }

        /**
         * Note: this class has a natural ordering that is inconsistent with equals.
         */
        @Override
        public int compareTo(PreviousResult other) {
            return Long.compare(this.timestamp, other.timestamp);
        }
    }
}
