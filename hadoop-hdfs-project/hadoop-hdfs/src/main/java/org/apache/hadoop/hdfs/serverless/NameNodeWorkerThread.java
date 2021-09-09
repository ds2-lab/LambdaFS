package org.apache.hadoop.hdfs.serverless;

import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
    private final BlockingQueue<FileSystemTask<? extends Serializable>> workQueue;

    public NameNodeWorkerThread(BlockingQueue<FileSystemTask<? extends Serializable>> workQueue,
                                ServerlessNameNode serverlessNameNodeInstance) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.workQueue = workQueue;
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread has started running.");

        try {
            while(true) {
                FileSystemTask<? extends Serializable> task = workQueue.take();

                LOG.debug("Worker thread: dequeued task " + task.getTaskId() + "(operation = "
                                + task.getOperationName() + ").");

                // Do something with the task.
            }
        }
        catch (InterruptedException ex) {
            LOG.error("Serverless NameNode Worker Thread was interrupted while running:", ex);
        }
    }
}
