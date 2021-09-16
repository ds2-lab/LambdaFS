package org.apache.hadoop.hdfs.serverless.operation;

import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.tcpserver.NameNodeTCPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
     * All tasks that are currently being executed. This is probably just going to be one at a time.
     */
    private final ConcurrentHashMap<String, FileSystemTask<Serializable>> currentlyExecutingTasks;

    /**
     * All tasks that have been executed by this worker thread.
     */
    private final ConcurrentHashMap<String, FileSystemTask<Serializable>> completedTasks;

    public NameNodeWorkerThread(BlockingQueue<FileSystemTask<Serializable>> workQueue,
                                ServerlessNameNode serverlessNameNodeInstance) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.workQueue = workQueue;

        this.currentlyExecutingTasks = new ConcurrentHashMap<>();
        this.completedTasks = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread has started running.");

        FileSystemTask<Serializable> task = null;
        try {
            while(true) {
                task = workQueue.poll(5000, TimeUnit.MILLISECONDS);

                if (task == null) {
                    LOG.debug("Worker thread did not find anything to do...");

                    NameNodeTCPClient nameNodeTCPClient = serverlessNameNodeInstance.getNameNodeTcpClient();

                    LOG.debug("======== NameNode TCP Client Debug Information ========");
                    LOG.debug("Number of clients: " + nameNodeTCPClient.numClients());
                    nameNodeTCPClient.checkThatClientsAreAllConnected();
                    LOG.debug("=======================================================");

                    continue;
                }

                LOG.debug("Worker thread: dequeued task " + task.getTaskId() + "(operation = "
                                + task.getOperationName() + ").");

                // Check if this is a duplicate task. If it is, then just discard it and move on.
                if (isTaskDuplicate(task)) {
                    LOG.warn("Encountered duplicate task " + task.getTaskId() + " (operation = "
                            + task.getOperationName() + ").");
                    continue;
                }

                currentlyExecutingTasks.put(task.getTaskId(), task);

                Serializable result = null;
                synchronized (serverlessNameNodeInstance) {
                    result = serverlessNameNodeInstance.performOperation(task.getOperationName(), task.getOperationArguments());
                }

                currentlyExecutingTasks.remove(task.getTaskId());

                // Do something with the task.
                if (result != null)
                    task.postResult(result);
                else
                    task.postResult(NullResult.getInstance());

                completedTasks.put(task.getTaskId(), task);
            }
        }
        catch (InterruptedException ex) {
            LOG.error("Serverless NameNode Worker Thread was interrupted while running:", ex);
        } catch (IOException | ClassNotFoundException | InvocationTargetException |
                NoSuchMethodException | IllegalAccessException ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName() + " while executing task " +
                    task.getTaskId() + " (operation = " + task.getOperationName() + ").", ex);
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
     * @param candidate the task for which we are checking if it is a duplicate
     * @return true if the task is a duplicate, otherwise false.
     */
    public synchronized boolean isTaskDuplicate(FileSystemTask<Serializable> candidate) {
        return currentlyExecutingTasks.containsKey(candidate.getTaskId())
                || completedTasks.containsKey(candidate.getTaskId());
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
     * Used to return 'null' for a FileSystemTask. If this is the result that is obtained, then
     * null is returned in place of an actual object (i.e., something that implements Serializable).
     */
    public static class NullResult implements Serializable {
        private static final long serialVersionUID = -1663521618378958991L;

        static NullResult instance;

        public static NullResult getInstance() {
            if (instance == null)
                instance = new NullResult();

            return instance;
        }
    }
}
