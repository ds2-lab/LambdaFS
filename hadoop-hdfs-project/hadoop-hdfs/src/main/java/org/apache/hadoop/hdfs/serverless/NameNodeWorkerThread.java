package org.apache.hadoop.hdfs.serverless;

import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;

/**
 * This thread actually executes file system operations. Tasks (i.e., file system operations wrapped in a Future
 * interface) are added to a work queue. This thread consumes those tasks and returns results to whomever is
 * waiting on them (one of the HTTP or TCP request handlers).
 */
public class NameNodeWorkerThread implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(NameNodeWorkerThread.class.getName());

    /**
     * Reference to the Serverless NameNode instance created in the OpenWhiskHandler class.
     */
    private final ServerlessNameNode serverlessNameNodeInstance;

    /**
     * HTTP and TCP requests will add work to this queue.
     */
    private final BlockingQueue<FileSystemTask<Serializable>> workQueue;

    public NameNodeWorkerThread(BlockingQueue<FileSystemTask<Serializable>> workQueue,
                                ServerlessNameNode serverlessNameNodeInstance) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.workQueue = workQueue;
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread has started running.");

        FileSystemTask<Serializable> task = null;
        try {
            while(true) {
                task = workQueue.take();

                LOG.debug("Worker thread: dequeued task " + task.getTaskId() + "(operation = "
                                + task.getOperationName() + ").");

                Serializable result = null;
                synchronized (serverlessNameNodeInstance) {
                    result = serverlessNameNodeInstance.performOperation(task.getOperationName(), task.getOperationArguments());
                }

                // Do something with the task.
                if (result != null)
                    task.postResult(result);
                else
                    task.postResult(NullResult.getInstance());
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
