package org.apache.hadoop.hdfs.serverless;

import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
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
            String functionUri,
            HashMap<String, Object> nameNodeArguments,
            HashMap<String, Object> fileSystemOperationArguments) throws IOException;
}