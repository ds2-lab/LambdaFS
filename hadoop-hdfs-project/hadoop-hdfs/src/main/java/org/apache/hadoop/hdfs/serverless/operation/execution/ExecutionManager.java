package org.apache.hadoop.hdfs.serverless.operation.execution;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResultWithMetrics;
import org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments.TaskArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * This class maintains much of the state used when executing file system operations on behalf of HopsFS clients.
 *
 * Specifically, this class owns the Work Queue as well as several internal maps that track which tasks are
 * currently being executed, which tasks have already been executed, etc.
 *
 * There should only be one ExecutionManager instance at any given time.
 */
public class ExecutionManager {
    //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);

    /**
     * Reference to the Serverless NameNode instance created in the OpenWhiskHandler class.
     */
    private final ServerlessNameNode serverlessNameNodeInstance;

//    /**
//     * All tasks that are currently being executed. For now, we only ever execute one task at a time.
//     */
    //private final ConcurrentHashMap<String, FileSystemTask<Serializable>> currentlyExecutingTasks;
    //private final Cache<String, FileSystemTask<Serializable>> currentlyExecutingTasks;
    //private Set<String> currentlyExecutingTasks;

//    /**
//     * All tasks that have been executed by this worker thread.
//     */
    //private final ConcurrentHashMap<String, FileSystemTask<Serializable>> completedTasks;
    //private final Cache<String, FileSystemTask<Serializable>> completedTasks;
    //private Set<String> completedTasks;

    /**
     * All the tasks we're either executing or have successfully executed.
     */
    private Set<String> seenTasks;

    /**
     * Cache of previously-computed results. These results are kept in-memory for a configurable period of time
     * so that they may be re-submitted to a client if the client does not receive the original transmission.
     *
     * The NameNode periodically removes a given PreviousResult from its cache after a configurable amount of time
     * has passed.
     *
     * This particular variable is used to quickly retrieve a previous result by requestId/taskId.
     *
     * NOTE: This IS thread safe.
     */
    //private final Cache<String, PreviousResult> previousResultCache;

    /**
     * Threads executing the consistency protocol place the {@link WriteAcknowledgement} objects they created into this
     * queue. The thread that periodically retrieves {@link io.hops.metadata.hdfs.entity.StorageReport} instances from
     * NDB will also routinely delete from NDB all the ACKs in this queue.
     */
    private final BlockingQueue<WriteAcknowledgement> writeAcknowledgementsToDelete;

    /**
     * Controls access to the {@code writeAcknowledgementsToDelete} object. While {@code writeAcknowledgementsToDelete}
     * is of type BlockingQueue and is therefore thread safe, the behavior of its drainTo() method is undefined if the
     * collection is modified concurrently. Thus, we want to exclusively lock the queue when we're performing drainTo().
     *
     * To achieve this, adding elements to {@code writeAcknowledgementsToDelete} requires a read lock, which is shared.
     * Before calling drainTo() on writeAcknowledgementsToDelete, we take an exclusive write lock to prevent other
     * objects from being added to {@code writeAcknowledgementsToDelete} while we are draining it.
     */
    private final ReadWriteLock writeAcknowledgementsToDeleteLock;

    /**
     * If True, then we include TX events on the result. If False, then we do not.
     */
    private final boolean txEventsEnabled;

    /**
     * Records the timestamp of the last time we executed a task. We use this to determine
     * if we should trigger a garbage collection ourselves (i.e., while we're idle).
     */
    private AtomicLong lastTaskExecutedTimestamp = new AtomicLong(0L);

    /**
     * Used to reset the value of {@code gcTriggeredDuringThisIdleInterval}. Specifically, if this value is not
     * equal to {@code lastTaskExecutedTimestamp}, then we're in a different idle interval (since we've executed a
     * task since the last time we checked). Thus, we set {@code gcTriggeredDuringThisIdleInterval} to false.
     */
    private long lastTaskExecutedTimestampCopy = 0L;

    /**
     * The amount of time (in milliseconds) that must elapse before we manually trigger a GC.
     */
    private final long idleGcThreshold;

    /**
     * We only need/want to trigger a GC once during an idle interval.
     * This flag indicates whether we've triggered one or not during an idle interval.
     */
    private volatile boolean gcTriggeredDuringThisIdleInterval = false;

    /**
     * We only want to perform NDB updates roughly every heartbeat interval. The update thread wakes up more
     * often than that to check for System.GC() calls, though. So, we use this to avoid checking NDB too often.
     */
    private long lastNdbUpdateTimestamp = 0L;

    public ExecutionManager(Configuration conf, ServerlessNameNode serverlessNameNode) {
        LOG.debug("Creating ExecutionManager now.");

        this.txEventsEnabled = conf.getBoolean(SERVERLESS_TX_EVENTS_ENABLED, SERVERLESS_TX_EVENTS_ENABLED_DEFAULT);
        this.idleGcThreshold = conf.getLong(SERVERLESS_IDLE_GC_THRESHOLD, SERVERLESS_IDLE_GC_THRESHOLD_DEFAULT);

//        int resultRetainIntervalMilliseconds = conf.getInt(SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS, SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS_DEFAULT);
//        int previousResultCacheMaxSize = conf.getInt(SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE, SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE_DEFAULT);

//        this.previousResultCache = Caffeine.newBuilder()
//                .maximumSize(previousResultCacheMaxSize)
//                .initialCapacity((int)(previousResultCacheMaxSize * 0.5))
//                .expireAfterWrite(Duration.ofMillis(resultRetainIntervalMilliseconds))
//                .expireAfterAccess(Duration.ofMillis(resultRetainIntervalMilliseconds))
//                .build();

//        this.currentlyExecutingTasks = new ConcurrentHashMap<>();
//        this.completedTasks = new ConcurrentHashMap<>();
//        this.currentlyExecutingTasks = Caffeine.newBuilder()
//                .expireAfterWrite(45, TimeUnit.SECONDS)
//                .maximumSize(5_000)
//                .build();
//        this.completedTasks = Caffeine.newBuilder()
//                .expireAfterWrite(45, TimeUnit.SECONDS)
//                .maximumSize(5_000)
//                .build();
//        this.currentlyExecutingTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
//        this.completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.seenTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.serverlessNameNodeInstance = serverlessNameNode;
        this.writeAcknowledgementsToDelete = new LinkedBlockingQueue<>();
        this.writeAcknowledgementsToDeleteLock = new ReentrantReadWriteLock();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::updateThread, 0, idleGcThreshold, TimeUnit.MILLISECONDS);
        //}, 0, serverlessNameNode.getHeartBeatInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * Called by the 'update thread'. Performs work such as fetching storage reports from NDB, clearing
     * ACKs from consistency protocol operations, and checking if we should manually trigger a GC.
     */
    private void updateThread() {
        if (System.currentTimeMillis() - lastNdbUpdateTimestamp > serverlessNameNodeInstance.getHeartBeatInterval()) {
            tryUpdateFromIntermediateStorage();

            // COMMENTED OUT: We use ZooKeeper for ACKs now, so we don't need to bother checking with NDB.
//            try {
//                removeWriteAcknowledgements();
//            } catch (StorageException ex) {
//                LOG.error("StorageException encountered while trying to delete old WriteAcknowledgement instances:", ex);
//            }

            lastNdbUpdateTimestamp = System.currentTimeMillis();
        }

        // We're going to check if we've been idle for long enough to want to trigger a GC.
        long lastTaskExecutedTs = lastTaskExecutedTimestamp.get();

        // If `lastTaskExecutedTimestampCopy` is not equal to lastTaskExecutedTimestamp, then we're in a different
        // idle interval than we were before (since we've executed a task since the last time we checked). Thus, we
        // set gcTriggeredDuringThisIdleInterval to false.
        if (lastTaskExecutedTs != lastTaskExecutedTimestampCopy) {
            // If debug logging is enabled, then we might print a message about resetting the value of the
            // `gcTriggeredDuringThisIdleInterval` flag. We only print a message if the flag is currently set
            // to true, and thus we're actually changing its value by setting it to false. If it is already
            // set to false, then we do not need to print a message.
            if (LOG.isDebugEnabled()) {
                if (gcTriggeredDuringThisIdleInterval)
                    LOG.debug("Resetting the value of 'gcTriggeredDuringThisIdleInterval'.");
            }

            gcTriggeredDuringThisIdleInterval = false;
        }

        // Have we been idle long enough to warrant a GC? If so, then trigger one,
        // assuming we haven't already triggered a GC in this same idle interval.
        if (System.currentTimeMillis() - lastTaskExecutedTs > idleGcThreshold && !gcTriggeredDuringThisIdleInterval) {
            LOG.debug("Manually triggering garbage collection!");
            System.gc();
            gcTriggeredDuringThisIdleInterval = true;
        }

        lastTaskExecutedTimestampCopy = lastTaskExecutedTs;
    }

    /**
     * Attempt to execute the file system operation submitted with the current request.
     *
     * @param taskId The unique ID of the request/task that we're executing.
     * @param operationName The name of the file system operation that we're executing.
     * @param taskArguments The arguments for the file system operation.
     * @param forceRedo If True, we should re-execute the task, even if we've executed it before.
     * @param workerResult Encapsulates all the information we will eventually return to the client.
     * @param http If true, we were invoked via HTTP. If false, then we were "invoked" via TCP.
     */
    public void tryExecuteTask(String taskId, String operationName, TaskArguments taskArguments, boolean forceRedo,
                               NameNodeResultWithMetrics workerResult, boolean http) {
        boolean duplicate = isTaskDuplicate(taskId);

        if (duplicate && !forceRedo) {
            // Technically we aren't dequeue-ing the task now, but we will never enqueue it since it is a duplicate.
            workerResult.addResult(new DuplicateRequest("TCP", taskId), true);
            workerResult.setDequeuedTime(System.currentTimeMillis());
            return;
        } else {
            // currentlyExecutingTasks.put(taskId, task);
            // currentlyExecutingTasks.add(taskId);
            seenTasks.add(taskId);
        }

        LOG.info("Executing task " + taskId + ", operation: " + operationName + " now.");

        workerResult.setDequeuedTime(System.currentTimeMillis());

        Serializable result;
        try {
            result = serverlessNameNodeInstance.performOperation(operationName, taskArguments);

            if (result == null)
                workerResult.addResult(NullResult.getInstance(), true);
            else
                workerResult.addResult(result, true);

            workerResult.setProcessingFinishedTime(System.currentTimeMillis());

            // We only add the task to the `completedTasks` mapping if we executed it successfully.
            // If there was an error, then may be automatically re-submitted by the client.
            lastTaskExecutedTimestamp.set(System.currentTimeMillis());
        } catch (Exception ex) {
            LOG.error("Encountered exception while executing file system operation " + operationName +
                    " for task " + taskId + ".", ex);
            workerResult.addException(ex);
            workerResult.setProcessingFinishedTime(System.currentTimeMillis());
        } catch (Throwable t) {
            LOG.error("Encountered throwable while executing file system operation " + operationName +
                    " for task " + taskId + ".", t);
            workerResult.addThrowable(t);
            workerResult.setProcessingFinishedTime(System.currentTimeMillis());
        }

        // If this task was submitted via HTTP, then attempt to create a deployment mapping.
        if (http) {
            try {
                long start = System.currentTimeMillis();
                // Check if a function mapping should be created and returned to the client.
                OpenWhiskHandler.tryCreateDeploymentMapping(workerResult, taskArguments, serverlessNameNodeInstance);
                long end = System.currentTimeMillis();

                if (LOG.isDebugEnabled())
                    LOG.debug("Created function mapping for request " + taskId + " in " + (end - start) + " ms.");
            } catch (IOException ex) {
                LOG.error("Encountered IOException while trying to create function mapping for task " +
                        taskId + ":", ex);
                workerResult.addException(ex);
            }
        }

        // The TX events are ThreadLocal, so we'll only get events generated by our thread.
        if (txEventsEnabled)
            workerResult.commitTransactionEvents(serverlessNameNodeInstance.getAndClearTransactionEvents());
    }

    /**
     * Attempt to execute the file system operation submitted with the current request.
     *
     * @param taskId The unique ID of the request/task that we're executing.
     * @param operationName The name of the file system operation that we're executing.
     * @param taskArguments The arguments for the file system operation.
     * @param forceRedo If True, we should re-execute the task, even if we've executed it before.
     * @param workerResult Encapsulates all the information we will eventually return to the client.
     * @param http If true, we were invoked via HTTP. If false, then we were "invoked" via TCP.
     */
    public void tryExecuteTask(String taskId, String operationName, TaskArguments taskArguments,
                               boolean forceRedo, NameNodeResult workerResult, boolean http) {
        boolean duplicate = isTaskDuplicate(taskId);

        if (duplicate && !forceRedo) {
            // TODO: Just mark the request as duplicate using a boolean flag.
            workerResult.addResult(new DuplicateRequest("TCP", taskId), true);
            return;
        } else {
            // currentlyExecutingTasks.put(taskId, task);
            // currentlyExecutingTasks.add(taskId);
            seenTasks.add(taskId);
        }

        LOG.info("Executing task " + taskId + ", operation: " + operationName + " now.");

        Serializable result = null;
        try {
            result = serverlessNameNodeInstance.performOperation(operationName, taskArguments);

            if (result == null)
                workerResult.addResult(NullResult.getInstance(), true);
            else
                workerResult.addResult(result, true);

            // We only add the task to the `completedTasks` mapping if we executed it successfully.
            // If there was an error, then may be automatically re-submitted by the client.
            lastTaskExecutedTimestamp.set(System.currentTimeMillis());
        } catch (Exception ex) {
            LOG.error("Encountered exception while executing file system operation " + operationName +
                    " for task " + taskId + ".", ex);
            workerResult.addException(ex);
        } catch (Throwable t) {
            LOG.error("Encountered throwable while executing file system operation " + operationName +
                    " for task " + taskId + ".", t);
            workerResult.addThrowable(t);
        }

        // If this task was submitted via HTTP, then attempt to create a deployment mapping.
        if (http) {
            try {
                long start = System.currentTimeMillis();
                // Check if a function mapping should be created and returned to the client.
                OpenWhiskHandler.tryCreateDeploymentMapping(workerResult, taskArguments, serverlessNameNodeInstance);
                long end = System.currentTimeMillis();

                if (LOG.isDebugEnabled())
                    LOG.debug("Created function mapping for request " + taskId + " in " + (end - start) + " ms.");
            } catch (IOException ex) {
                LOG.error("Encountered IOException while trying to create function mapping for task " +
                        taskId + ":", ex);
                workerResult.addException(ex);
            }
        }
    }


    /**
     * Schedule all of the {@link WriteAcknowledgement} instances in {@code acksToDelete} to be removed from
     * intermediate storage (e.g., NDB).
     */
    public void enqueueAcksForDeletion(Collection<WriteAcknowledgement> acksToDelete) {
        Lock readLock = writeAcknowledgementsToDeleteLock.readLock();
        readLock.lock();
        try {
            writeAcknowledgementsToDelete.addAll(acksToDelete);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove all {@link WriteAcknowledgement} instances contained within the {@code writeAcknowledgementsToDelete}
     * queue from intermediate storage (NDB).
     *
     * An exclusive lock is taken temporarily on the {@code writeAcknowledgementsToDelete} instance so that
     * drainTo() may be called without risk of concurrent modification to the queue.
     */
    private void removeWriteAcknowledgements() throws StorageException {
        WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
                (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);

        ArrayList<WriteAcknowledgement> acksToDelete = new ArrayList<>();
        Lock writeLock = writeAcknowledgementsToDeleteLock.writeLock();
        writeLock.lock();
        try {
            writeAcknowledgementsToDelete.drainTo(acksToDelete);
        } finally {
            writeLock.unlock();
        }

        writeAcknowledgementDataAccess.deleteAcknowledgements(acksToDelete);
    }

    /**
     * Try to retrieve and process updates from intermediate storage.
     * These updates include storage reports and intermediate block reports.
     *
     * This was originally performed exclusively in the HTTP handler
     * {@link org.apache.hadoop.hdfs.serverless.OpenWhiskHandler}. We moved it here because, when clients were issuing
     * TCP requests to NameNodes rather than HTTP requests, we would encounter errors as the NNs never retrieved and
     * processed storage reports and intermediate block reports. Now there is a dedicated thread that periodically
     * retrieves and processes these updates.
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

//    /**
//     * Update internal state that tracks whether a particular task has been completed or not.
//     *
//     * @param task The {@link FileSystemTask} instance that we are marking as completed.
//     */
//    public void taskCompleted(FileSystemTask<Serializable> task) {
//        taskCompleted(task.getTaskId());
//    }
//
//    /**
//     * Update internal state that tracks whether a particular task has been completed or not.
//     *
//     * @param taskId The unique ID of the {@link FileSystemTask} instance that we are marking as completed.
//     */
//    public void taskCompleted(String taskId) {
//        if (taskId == null)
//            throw new IllegalArgumentException("The provided FileSystemTask ID should not be null.");
//
//        // Put it in 'completed tasks' first so that, if we check for duplicate while modifying all this state,
//        // we'll correctly return true. If we removed it from 'currently executing tasks' first, then there could
//        // be a race where the task has been removed from 'currently executing tasks' but not yet added to 'completed
//        // tasks' yet, which could result in false negatives when checking for duplicates.
//        //completedTasks.put(taskId, task);
//        //currentlyExecutingTasks.asMap().remove(taskId);
//        //completedTasks.add(taskId);
//        //currentlyExecutingTasks.remove(taskId);
//        lastTaskExecutedTimestamp.set(System.currentTimeMillis()); // Update the time at which we last executed a task.
//    }

//    /**
//     * Store the result {@code previousResult} of the task identified by {@code taskId}.
//     * @param taskId The unique ID of the task whose execution generated the result encapsulated by {@code previousResult}.
//     * @param previousResult The result generated by executing the task whose ID is {@code taskId}.
//     */
//    public synchronized void cachePreviousResult(String taskId, PreviousResult previousResult) {
//        this.previousResultCache.put(taskId, previousResult);
//    }

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
    public synchronized boolean isTaskDuplicate(String taskId) {
        //return currentlyExecutingTasks.asMap().containsKey(taskId) || completedTasks.asMap().containsKey(taskId);
        //return currentlyExecutingTasks.containsKey(taskId) || completedTasks.containsKey(taskId);
        // return currentlyExecutingTasks.contains(taskId) || completedTasks.contains(taskId);
        return seenTasks.contains(taskId);
    }

//    /**
//     * Handler for when the worker thread encounters a duplicate task.
//     * @param task The task in question.
//     * @param result The NameNodeResult object being used by the worker thread.
//     */
//    private boolean handleDuplicateTask(FileSystemTask<Serializable> task, NameNodeResult result) {
//        LOG.warn("Encountered duplicate task " + task.getTaskId() + " (operation = " + task.getOperationName() + ").");
//
//        if (task.getForceRedo()) {
//            LOG.warn("Client did not receive the original transmission of the result for "
//                    + task.getTaskId() + ". Checking to see if result is cached...");
//
//            // This IS thread safe.
//            PreviousResult previousResult = previousResultCache.getIfPresent(task.getTaskId());
//            if (previousResult != null) {
//                if (LOG.isDebugEnabled()) LOG.debug("Result for task " + task.getTaskId() + " is still cached. Returning it to the client now.");
//            } else {
//                LOG.warn("Result for task " + task.getTaskId() + " is no longer in the cache.");
//
//                result.addResult(new DuplicateRequest("TCP", task.getTaskId()), true);
//
//                // Simply post a DuplicateRequest message. The client-side code will know that this means
//                // that the result is no longer in the cache, and the operation must be restarted.
//            }
//        } else {
//            result.addResult(new DuplicateRequest("TCP", task.getTaskId()), true);
//        }
//        return result;
//    }
}
