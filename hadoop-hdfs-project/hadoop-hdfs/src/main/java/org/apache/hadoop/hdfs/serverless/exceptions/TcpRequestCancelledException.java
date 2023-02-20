package org.apache.hadoop.hdfs.serverless.exceptions;

import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessHttpFuture;

/**
 * Thrown when a TCP request gets cancelled (due to connection lost).
 */
public class TcpRequestCancelledException extends Exception {
    private static final long serialVersionUID = 0xcd8ac329L;

    private final ServerlessHttpFuture resubmissionFuture;

    public TcpRequestCancelledException() {
        this(null, null, null);
    }

    public TcpRequestCancelledException(String message, ServerlessHttpFuture resubmissionFuture) {
        this(message, null, resubmissionFuture);
    }

    public TcpRequestCancelledException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public TcpRequestCancelledException(String message, Throwable cause, ServerlessHttpFuture resubmissionFuture) {
        super(message, cause);

        this.resubmissionFuture = resubmissionFuture;
    }

    public ServerlessHttpFuture getResubmissionFuture() {
        return resubmissionFuture;
    }
}
