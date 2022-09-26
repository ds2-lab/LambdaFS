package org.apache.hadoop.hdfs.serverless.exceptions;

import java.io.Serializable;

/**
 * Wrap another exception so that it can be serialized without being too huge (due to stack trace).
 */
public class NameNodeException implements Serializable {
    private static final long serialVersionUID = -4872084057395323848L;

    private final String trueExceptionName;
    private final String message;

    public NameNodeException(String message, String trueExceptionName) {
        this.message = message;
        this.trueExceptionName = trueExceptionName;
    }

    public String getTrueExceptionName() {
        return trueExceptionName;
    }

    public String getMessage() { return message; }

    @Override
    public String toString() {
        return "NameNodeException(" + trueExceptionName + "): " + message;
    }
}
