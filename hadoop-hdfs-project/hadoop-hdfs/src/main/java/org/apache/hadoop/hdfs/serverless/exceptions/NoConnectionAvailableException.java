package org.apache.hadoop.hdfs.serverless.exceptions;

import java.io.IOException;

/**
 * Thrown when we are trying to issue a TCP/UDP request and there are unexpectedly no available connections.
 *
 * Specifically, we get to a point in the process where we had previously believed there to be at least one
 * available, viable connection. But now that we're about to issue the request, that connection is no longer
 * present.
 */
public class NoConnectionAvailableException extends IOException {
    private static final long serialVersionUID = -4872084057395323848L;

    public NoConnectionAvailableException(String str) {
        super(str);
    }

    public NoConnectionAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
