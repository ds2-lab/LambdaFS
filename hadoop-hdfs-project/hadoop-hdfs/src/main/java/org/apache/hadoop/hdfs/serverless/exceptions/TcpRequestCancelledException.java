package org.apache.hadoop.hdfs.serverless.exceptions;

import java.io.IOException;

/**
 * Thrown when a TCP request gets cancelled (due to connection lost).
 */
public class TcpRequestCancelledException extends IOException {
    private static final long serialVersionUID = 0xcd8ac329L;

    public TcpRequestCancelledException(String str) {
        super(str);
    }

    public TcpRequestCancelledException(String message, Throwable cause) {
        super(message, cause);
    }
}
