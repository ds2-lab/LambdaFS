package org.apache.hadoop.hdfs.serverless.execution.futures;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.LOCAL_EXCEPTION;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.REQUEST_ID;

/**
 * These are created when issuing HTTP requests to NameNodes. These are used on the client side.
 */
public class ServerlessHttpFuture extends ServerlessFuture<JsonObject> {
    private static final Log LOG = LogFactory.getLog(ServerlessHttpFuture.class);
    public ServerlessHttpFuture(String requestId, String operationName) {
        super(requestId, operationName);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        LOG.debug("ServerlessHttpFuture for request " + requestId + " is being cancelled...");
        JsonObject cancellationMessage = new JsonObject();
        cancellationMessage.addProperty(REQUEST_ID, requestId);
        cancellationMessage.addProperty(LOCAL_EXCEPTION, "IOException");

        try {
            resultQueue.put(cancellationMessage);
        } catch (InterruptedException e) {
            LOG.error("Exception encountered while cancelling future for request " + requestId + ":", e);

            // TODO: What now? Fail silently? Seems bad.
            return false;
        }

        return true;
    }

    @Override
    public void cancel(String reason, boolean shouldRetry) throws InterruptedException {
        LOG.debug("ServerlessHttpFuture for request " + requestId + " is being cancelled using incorrect API...");
        throw new NotImplementedException("cancel(String, boolean) is not supported for HTTP futures.");
    }
}
