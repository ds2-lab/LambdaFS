package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
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

    /**
     * All tasks that are currently being executed. For now, we only ever execute one task at a time.
     */
    //private final ConcurrentHashMap<String, FileSystemTask<Serializable>> currentlyExecutingTasks;
    //private final Cache<String, FileSystemTask<Serializable>> currentlyExecutingTasks;
    private Set<String> currentlyExecutingTasks;

    /**
     * All tasks that have been executed by this worker thread.
     */
    //private final ConcurrentHashMap<String, FileSystemTask<Serializable>> completedTasks;
    //private final Cache<String, FileSystemTask<Serializable>> completedTasks;
    private Set<String> completedTasks;

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

    public ExecutionManager(Configuration conf, ServerlessNameNode serverlessNameNode) {
        LOG.debug("Creating ExecutionManager now.");

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
        this.currentlyExecutingTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.serverlessNameNodeInstance = serverlessNameNode;
        this.writeAcknowledgementsToDelete = new LinkedBlockingQueue<>();
        this.writeAcknowledgementsToDeleteLock = new ReentrantReadWriteLock();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            tryUpdateFromIntermediateStorage();

            try {
                removeWriteAcknowledgements();
            } catch (StorageException ex) {
                LOG.error("StorageException encountered while trying to delete old WriteAcknowledgement instances:", ex);
            }
        }, 0, serverlessNameNode.getHeartBeatInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * Attempt to execute the file system operation submitted with the current request.
     *
     * @param task The task we're executing.
     * @param workerResult The result object to be returned to the client.
     * @param http True if this request was issued via HTTP. If false, then request was issued via TCP.
     */
    public void tryExecuteTask(FileSystemTask<Serializable> task, NameNodeResult workerResult, boolean http) {
        boolean duplicate = isTaskDuplicate(task);

        // If this is a duplicate task, and we aren't being forced to redo it, then return it to the client.
        // They'll have to resubmit it if they need it to get done again.
        // TODO: Is it necessary to have the client resubmit?
        //       We previously used to cache results and then return them, but we stopped doing that
        //       as it was seldom used (and just used up a lot of memory). If we find this is an issue,
        //       then we can just start using the PreviousResultsCache again. In general, this construct
        //       just doesn't make sense. We see a duplicate, send it back, and then execute it again if the
        //       client re-submits. The fact that we're seeing a duplicate probably means the client didn't get it
        //       though, so this whole process just seems to waste time. (But again, it actually doesn't seem
        //       relevant as it rarely happens with the current way of handling requests [i.e., no dual HTTP-TCP
        //       anymore]), so for now, we'll just leave this as is (in the interest of time).
        if (duplicate && !task.getForceRedo()) {
            // Technically we aren't dequeue-ing the task now, but we will never enqueue it since it is a duplicate.
            workerResult.setDequeuedTime(System.currentTimeMillis());
            workerResult.addResult(new DuplicateRequest("TCP", task.getTaskId()), true);
            return;
        } else {
            // currentlyExecutingTasks.put(task.getTaskId(), task);
            currentlyExecutingTasks.add(task.getTaskId());
        }

        LOG.info("Executing task " + task.getTaskId() + ", operation: " + task.getOperationName() + " now.");

        workerResult.setDequeuedTime(System.currentTimeMillis());

        Serializable result = null;
        boolean success = false;
        try {
            result = serverlessNameNodeInstance.performOperation(task.getOperationName(), task.getOperationArguments());

            if (result == null)
                workerResult.addResult(NullResult.getInstance(), true);
            else
                workerResult.addResult(result, true);

            workerResult.setProcessingFinishedTime(System.currentTimeMillis());
            success = true;
        } catch (Exception ex) {
            LOG.error("Encountered exception while executing file system operation " + task.getOperationName() +
                    " for task " + task.getTaskId() + ".", ex);
            workerResult.addException(ex);
            workerResult.setProcessingFinishedTime(System.currentTimeMillis());
        } catch (Throwable t) {
            LOG.error("Encountered throwable while executing file system operation " + task.getOperationName() +
                    " for task " + task.getTaskId() + ".", t);
            workerResult.addThrowable(t);
            workerResult.setProcessingFinishedTime(System.currentTimeMillis());
        }

        // If this task was submitted via HTTP, then attempt to create a deployment mapping.
        if (http) {
            try {
                long start = System.currentTimeMillis();
                // Check if a function mapping should be created and returned to the client.
                OpenWhiskHandler.tryCreateDeploymentMapping(workerResult, task.getOperationArguments(), serverlessNameNodeInstance);
                long end = System.currentTimeMillis();

                if (LOG.isDebugEnabled())
                    LOG.debug("Created function mapping for request " + task.getTaskId() + " in " + (end - start) + " ms.");
            } catch (IOException ex) {
                LOG.error("Encountered IOException while trying to create function mapping for task " +
                        task.getTaskId() + ":", ex);
                workerResult.addException(ex);
            }
        }

        // The TX events are ThreadLocal, so we'll only get events generated by our thread.
        workerResult.commitTransactionEvents(serverlessNameNodeInstance.getTransactionEvents());
        serverlessNameNodeInstance.clearTransactionEvents();

        // We only add the task to the `completedTasks` mapping if we executed it successfully.
        // If there was an error, then may be automatically re-submitted by the client.
        if (success) {
            long s = System.currentTimeMillis();
            taskCompleted(task);
            long t = System.currentTimeMillis();

            // notifyTaskCompleted() modifies some ConcurrentHashMaps (or possibly Caffeine Cache objects now).
            // If these operations are taking a long time, then we log a warning about it. They should be fast though.
            if (t - s > 10)
                LOG.warn("Notifying completion of task " + task.getTaskId() + " took " + (t - s) + " ms.");
        }

//        long s = System.currentTimeMillis();
//        // Cache the result for a bit.
//        PreviousResult previousResult = new PreviousResult(result, task.getOperationName(), task.getTaskId());
//        cachePreviousResult(task.getTaskId(), previousResult);
//        long t = System.currentTimeMillis();
//
//        // notifyTaskCompleted() modifies some ConcurrentHashMaps (or possibly Caffeine Cache objects now).
//        // If these operations are taking a long time, then we log a warning about it. They should be fast though.
//        if (t - s > 10)
//            LOG.warn("Caching result of task " + task.getTaskId() + " took " + (t - s) + " ms.");
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

    /**
     * Update internal state that tracks whether a particular task has been completed or not.
     */
    public void taskCompleted(FileSystemTask<Serializable> task) {
        if (task == null)
            throw new IllegalArgumentException("The provided FileSystemTask object should not be null.");

        String taskId = task.getTaskId();

        // Put it in 'completed tasks' first so that, if we check for duplicate while modifying all this state,
        // we'll correctly return true. If we removed it from 'currently executing tasks' first, then there could
        // be a race where the task has been removed from 'currently executing tasks' but not yet added to 'completed
        // tasks' yet, which could result in false negatives when checking for duplicates.
        //completedTasks.put(taskId, task);
        //currentlyExecutingTasks.asMap().remove(taskId);
        completedTasks.add(taskId);
        currentlyExecutingTasks.remove(taskId);
    }

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
        return currentlyExecutingTasks.contains(taskId) || completedTasks.contains(taskId);
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
