package org.apache.hadoop.hdfs.serverless;

import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

public class ServerlessNameNodeWorkerThread implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(ServerlessNameNodeWorkerThread.class.getName());

    /**
     * Reference to the Serverless NameNode instance created in the OpenWhiskHandler class.
     */
    private final ServerlessNameNode serverlessNameNodeInstance;

    /**
     * HTTP and TCP requests will add work to this queue.
     */
    private final BlockingQueue<ServerlessNameNodeTask<? extends Serializable>> workQueue;

    public ServerlessNameNodeWorkerThread(BlockingQueue<ServerlessNameNodeTask<? extends Serializable>> workQueue,
                                          ServerlessNameNode serverlessNameNodeInstance) {
        this.serverlessNameNodeInstance = serverlessNameNodeInstance;
        this.workQueue = workQueue;
    }

    @Override
    public void run() {
        LOG.info("Serverless NameNode Worker Thread has started running.");

        try {
            while(true) {
                ServerlessNameNodeTask nextTask = workQueue.take();

                // Do something with the task.
            }
        }
        catch (InterruptedException ex) {
            LOG.error("Serverless NameNode Worker Thread was interrupted while running:", ex);
        }
    }
}
