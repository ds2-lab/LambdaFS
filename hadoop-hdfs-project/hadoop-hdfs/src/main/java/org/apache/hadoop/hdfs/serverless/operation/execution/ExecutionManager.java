package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.esotericsoftware.kryonet.Client;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
    public static final Logger LOG = LoggerFactory.getLogger(ExecutionManager.class.getName());

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
    private final Cache<String, PreviousResult> previousResultCache;

    /**
     * The ID of the NameNode that this thread is running in.
     */
    private final long nameNodeId;

    /**
     * The worker/handler threads.
     */
    private final List<NameNodeWorkerThread> workerThreads;

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
        int resultRetainIntervalMilliseconds = conf.getInt(SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS, SERVERLESS_RESULT_CACHE_INTERVAL_MILLISECONDS_DEFAULT);
        int previousResultCacheMaxSize = conf.getInt(SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE, SERVERLESS_RESULT_CACHE_MAXIMUM_SIZE_DEFAULT);
        int numWorkerThreads = conf.getInt(SERVERLESS_NUM_HANDLER_THREADS, SERVERLESS_NUM_HANDLER_THREADS_DEFAULT);

        this.workQueue = new LinkedBlockingQueue<>();
        this.previousResultCache = Caffeine.newBuilder()
                .maximumSize(previousResultCacheMaxSize)
                .initialCapacity((int)(previousResultCacheMaxSize * 0.5))
                .expireAfterWrite(Duration.ofMillis(resultRetainIntervalMilliseconds))
                .expireAfterAccess(Duration.ofMillis(resultRetainIntervalMilliseconds))
                .build();

        this.currentlyExecutingTasks = new ConcurrentHashMap<>();
        this.completedTasks = new ConcurrentHashMap<>();
        this.serverlessNameNodeInstance = serverlessNameNode;
        this.nameNodeId = serverlessNameNode.getId();
        this.writeAcknowledgementsToDelete = new LinkedBlockingQueue<>();
        this.writeAcknowledgementsToDeleteLock = new ReentrantReadWriteLock();

        this.workerThreads = new ArrayList<>();
        LOG.debug("Creating " + numWorkerThreads + " worker thread(s).");
        for (int i = 0; i < numWorkerThreads; i++) {
            NameNodeWorkerThread nameNodeWorkerThread = new NameNodeWorkerThread(serverlessNameNodeInstance,
                    this, i);
            workerThreads.add(nameNodeWorkerThread);
            nameNodeWorkerThread.start();
        }

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            LOG.debug("Retrieving StorageReports from intermediate storage now.");
            tryUpdateFromIntermediateStorage();

            try {
                removeWriteAcknowledgements();
            } catch (StorageException ex) {
                LOG.error("StorageException encountered while trying to delete old WriteAcknowledgement instances:", ex);
            }
        }, 0, serverlessNameNode.getHeartBeatInterval(), TimeUnit.MILLISECONDS);
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
     * Add work to the Work Queue. This will block if the queue is full, but presently, the queue has no
     * capacity constraint, so this should never block.
     *
     * @param task {@link FileSystemTask} object encapsulating the work to be performed.
     */
    public void putWork(FileSystemTask<Serializable> task) throws InterruptedException {
        boolean duplicate = isTaskDuplicate(task);

        if (duplicate) {
            NameNodeResult result = new NameNodeResult(this.serverlessNameNodeInstance.getDeploymentNumber(),
                    task.getTaskId(), task.getOperationName(), this.nameNodeId);
            // Technically we aren't dequeue-ing the task now, but we will never enqueue it since it is a duplicate.
            result.setDequeuedTime(Time.getUtcTime());
            handleDuplicateTask(task, result);
        } else {
            workQueue.put(task);
        }
    }

    /**
     * Attempt to retrieve a FileSystemTask from the Work Queue. This will block for the specified period of time.
     * If there is no work available by timeout, then this function returns null.
     *
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit a {@link TimeUnit} determining how to interpret the {@code timeout} parameter
     */
    public FileSystemTask<Serializable> getWork(long timeout, TimeUnit unit) throws InterruptedException {
        FileSystemTask<Serializable> task = this.workQueue.poll(timeout, unit);

        if (task != null)
            currentlyExecutingTasks.put(task.getTaskId(), task);

        return task;
    }

    /**
     * Attempt to retrieve a FileSystemTask from the Work Queue without blocking. If there is no work available,
     * then this function returns null.
     */
    public FileSystemTask<Serializable> getWork() throws InterruptedException {
        FileSystemTask<Serializable> task = this.workQueue.take();

        currentlyExecutingTasks.put(task.getTaskId(), task);

        return task;
    }

    /**
     * Used by {@link NameNodeWorkerThread} objects to notify the ExecutionManager that a task has been completed.
     */
    public void notifyTaskCompleted(FileSystemTask<Serializable> task) {
        if (task == null)
            throw new IllegalArgumentException("The provided FileSystemTask object should not be null.");

        // Atomically check and update this state.
        synchronized (this) {
            String taskId = task.getTaskId();
            if (!currentlyExecutingTasks.containsKey(taskId))
                throw new IllegalStateException("Task " + taskId + " is not currently executing.");

            currentlyExecutingTasks.remove(taskId);

            if (completedTasks.containsKey(taskId))
                throw new IllegalStateException("Task " + taskId + " (op=" + task.getOperationName() +
                        ") has just finished executing, yet it is already present in the completedTasks map...");

            completedTasks.put(taskId, task);
        }
    }

    /**
     * Store the result {@code previousResult} of the task identified by {@code taskId}.
     * @param taskId The unique ID of the task whose execution generated the result encapsulated by {@code previousResult}.
     * @param previousResult The result generated by executing the task whose ID is {@code taskId}.
     */
    public synchronized void cachePreviousResult(String taskId, PreviousResult previousResult) {
        LOG.debug("Caching result of task " + taskId + ".");
        this.previousResultCache.put(taskId, previousResult);
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
    public synchronized boolean isTaskDuplicate(String taskId) {
        return currentlyExecutingTasks.containsKey(taskId) || completedTasks.containsKey(taskId);
    }

    /**
     * Handler for when the worker thread encounters a duplicate task.
     * @param task The task in question.
     * @param result The NameNodeResult object being used by the worker thread.
     */
    private void handleDuplicateTask(FileSystemTask<Serializable> task, NameNodeResult result) {
        LOG.warn("Encountered duplicate task " + task.getTaskId() + " (operation = " + task.getOperationName() + ").");

        if (task.getForceRedo()) {
            LOG.warn("Client did not receive the original transmission of the result for "
                    + task.getTaskId() + ". Checking to see if result is cached...");

            // This IS thread safe.
            PreviousResult previousResult = previousResultCache.getIfPresent(task.getTaskId());
            if (previousResult != null) {
                LOG.debug("Result for task " + task.getTaskId() + " is still cached. Returning it to the client now.");
                task.postResult(previousResult.value);
            } else {
                LOG.warn("Result for task " + task.getTaskId() + " is no longer in the cache.");

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
}
