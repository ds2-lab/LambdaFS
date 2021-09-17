package io.hops.transaction.handler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by antonis on 8/29/16.
 */
public final class ThreadPool {

    private final int POOL_SIZE  = 60;

    private volatile static ThreadPool instance = null;
    private final ExecutorService commitThreadPool;

    private ThreadPool() {
        commitThreadPool = Executors.newFixedThreadPool(POOL_SIZE);
    }

    public static ThreadPool getInstance() {
        if (instance == null) {
            synchronized(ThreadPool.class) {
                if (instance == null) {
                    instance = new ThreadPool();
                }
            }
        }

        return instance;
    }

    public ExecutorService getCommitThreadPool() {
        return commitThreadPool;
    }
}
