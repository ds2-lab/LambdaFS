package org.apache.hadoop.hdfs.serverless.execution.results;

import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessHttpFuture;

/**
 * Used to cancel results.
 */
public class CancelledResult extends NameNodeResult {
    private static final long serialVersionUID = 6000749351001594894L;

//    public static CancelledResult getInstance() {
//        if (instance == null)
//            instance = new CancelledResult();
//
//        return instance;
//    }
//
//    private static CancelledResult instance;

    /**
     * The {@code ServerlessHttpFuture} object created when resubmitting the cancelled TCP request.
     *
     * If this is null, then the request must not have been resubmitted.
     */
    private final ServerlessHttpFuture httpFuture;

    public CancelledResult(ServerlessHttpFuture httpFuture) {
        this.httpFuture = httpFuture;
    }

    public ServerlessHttpFuture getHttpFuture() {
        return httpFuture;
    }
}
