package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import org.apache.hadoop.hdfs.serverless.ArgumentContainer;
import org.apache.hadoop.hdfs.serverless.cache.FunctionMetadataMap;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

/**
 * Interface that defines the API of a generic serverless function invoker.
 *
 * Concrete implementations of this interface should be provided for different serverless functions,
 * such a OpenWhisk, OpenFaaS, and Nuclio.
 *
 * This interface expects serverless functions to return objects of type T. In general, T will likely be
 * {@link com.google.gson.JsonObject}, but this interface is defined generically
 * to provide flexibility.
 */
public interface ServerlessInvoker<T> {
    /**
     * Invoke a serverless NameNode function via an HTTP POST request and return the response to the user.
     * @param operationName The name of the file system operation to be performed by the ServerlessNameNode.
     * @param functionUri The web-enabled URI of the serverless NameNode function. We issue an HTTP POST request
     *                    to this URI to invoke the function.
     * @param nameNodeArguments Any additional arguments to pass to the NameNode itself. This function will handle
     *                          adding the default/bare minimum/required arguments.
     * @param fileSystemOperationArguments The arguments to pass to the file system function.
     * @return The response/result of the NameNode serverless function.
     */
    public T invokeNameNodeViaHttpPost(
            String operationName,
            String functionUriBase,
            HashMap<String, Object> nameNodeArguments,
            HashMap<String, Object> fileSystemOperationArguments) throws IOException;

    public T invokeNameNodeViaHttpPost(
            String operationName,
            String functionUriBase,
            HashMap<String, Object> nameNodeArguments,
            ArgumentContainer fileSystemOperationArguments) throws IOException;

    /**
     * Return the FunctionMetadataMap instance associated with this serverless invoker.
     *
     * FunctionMetadataMap objects maintain a cache of file/directory-to-serverless-function mappings.
     */
    public FunctionMetadataMap getFunctionMetadataMap();

    /**
     * Return an HTTP client configured appropriately for the serverless platform associated with the
     * concrete implementation of this interface.
     */
    public CloseableHttpClient getHttpClient() throws NoSuchAlgorithmException, KeyManagementException;

    /**
     * Extract and return the result from the JsonObject response obtained from the HttpPost request used to invoke
     * the serverless name node.
     *
     * This function will also check for an Exception. If one is found, then it will be logged.
     * This function will also update the serverless function cache mapping automatically.
     * @param response The response obtained from the HttpPost request used to invoke the serverless name node.
     */
    public Object extractResultFromJsonResponse(JsonObject response) throws IOException, ClassNotFoundException;

    /**
     * Performs any necessary cleanup.
     */
    public void terminate();
}
