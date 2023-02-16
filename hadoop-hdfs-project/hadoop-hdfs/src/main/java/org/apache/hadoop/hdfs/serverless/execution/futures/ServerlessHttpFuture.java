package org.apache.hadoop.hdfs.serverless.execution.futures;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.execution.results.NullResult;
import org.apache.http.NoHttpResponseException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public JsonObject get() throws InterruptedException, ExecutionException {
        if (LOG.isDebugEnabled()) LOG.debug("Waiting for result for request " + requestId + " now...");
        final JsonObject response = this.resultQueue.take();
        if (LOG.isDebugEnabled()) LOG.debug("Got result for future " + requestId + ".");

        if (response.has(LOCAL_EXCEPTION)) {
            String genericExceptionType = response.get(LOCAL_EXCEPTION).getAsString();

            if (genericExceptionType.equalsIgnoreCase("NoHttpResponseException")) {
                throw new ExecutionException(new NoHttpResponseException("Target NameNode (or perhaps the FaaS platform itself) failed to respond with a valid HTTP response."));
            }
            else if (genericExceptionType.equalsIgnoreCase("SocketTimeoutException")) {
                throw new ExecutionException((new SocketTimeoutException("Timeout occurred during socket read or accept.")));
            } else {
                LOG.error("Unexpected error encountered while invoking NN via HTTP: " + genericExceptionType);

                // TODO(ben): This is gross. Maybe return the real exception from the NameNode?
                //            It's just that serializing the full exception can be expensive...
                throw new ExecutionException(new IOException("The file system operation could not be completed. "
                        + "Encountered unexpected " + genericExceptionType + " while invoking NN."));
            }
        }

        return response;
    }

    @Override
    public JsonObject get(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final JsonObject resultOrNull = this.resultQueue.poll(timeout, unit);
        if (resultOrNull == null) {
            throw new TimeoutException();
        }

        return resultOrNull;
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
