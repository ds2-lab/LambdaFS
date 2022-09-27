package org.apache.hadoop.hdfs.serverless.exceptions;

import java.io.IOException;

/**
 * Used when a NameNode receives an object via TCP/UDP that it does not support.
 * By "not support", it means that the NameNode does not know what to do with
 * the object. There is no case for how to process the object or how to respond.
 */
public class UnsupportedObjectPayloadException extends IOException {
    private static final long serialVersionUID = 7619938703621486149L;

    public UnsupportedObjectPayloadException() {}

    public UnsupportedObjectPayloadException(String message) {
        super(message);
    }

    public UnsupportedObjectPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
