package org.apache.hadoop.hdfs.serverless.execution.futures;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.NotImplementedException;

/**
 * These are created when issuing HTTP requests to NameNodes. These are used on the client side.
 */
public class ServerlessHttpFuture extends ServerlessFuture<JsonObject> {
    public ServerlessHttpFuture(String requestId, String operationName) {
        super(requestId, operationName);
    }

    @Override
    public void cancel(String reason, boolean shouldRetry) throws InterruptedException {
        throw new NotImplementedException("The cancel API is not supported for HTTP futures.");
    }
}
