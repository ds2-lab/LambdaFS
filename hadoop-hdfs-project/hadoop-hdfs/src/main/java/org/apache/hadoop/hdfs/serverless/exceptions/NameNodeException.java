package org.apache.hadoop.hdfs.serverless.exceptions;

/**
 * Wrap another exception so that it can be serialized without being too huge (due to stack trace).
 */
public class NameNodeException extends Throwable {
    private static final long serialVersionUID = -4872084057395323848L;

    private final String trueExceptionName;

    public NameNodeException(String message, String trueExceptionName) {
        super(message, null, true, false);

        this.trueExceptionName = trueExceptionName;
    }

    public String getTrueExceptionName() {
        return trueExceptionName;
    }

    @Override
    public String toString() {
        return "NameNodeException(" + trueExceptionName + "): " + this.getMessage();
    }
}
