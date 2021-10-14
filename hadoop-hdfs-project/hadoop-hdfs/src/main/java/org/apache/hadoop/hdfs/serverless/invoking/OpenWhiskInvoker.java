package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.NullResult;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Concrete implementation of the {@link ServerlessInvoker} interface for the OpenWhisk serverless platform.
 *
 * The serverless platform being used is specified in the configuration files for Serverless HopsFS. Currently, it
 * defaults to OpenWhisk. In order to obtain an invoker, you simply utilize the {@link ServerlessInvokerBase} class,
 * passing whatever platform is specified in the configuration. The factory will provide a concrete implementation
 * for the platform being used.
 */
public class OpenWhiskInvoker extends ServerlessInvokerBase<JsonObject> {
    private static final Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * This is appended to the end of the serverlessEndpointBase AFTER the number is added.
     */
    private final String blockingParameter = "?blocking=true";

    private final Random random = new Random();

    /**
     * The maximum amount of time to wait before issuing another HTTP request after the previous request failed.
     *
     * TODO: Make this configurable.
     */
    private static final int maxBackoffMilliseconds = 5000;

    /**
     * Because invokers are generally created via the {@link ServerlessInvokerFactory} class, this constructor
     * will not be used directly.
     */
    protected OpenWhiskInvoker() throws NoSuchAlgorithmException, KeyManagementException {
        super();
    }

    /**
     * Invoke a serverless NameNode function via HTTP POST, which is the standard/only way of "invoking" a serverless
     * function in Serverless HopsFS. (OpenWhisk supports HTTP GET, I think? But we don't use that, in any case.)
     *
     * This overload of the {@link ServerlessInvokerBase#invokeNameNodeViaHttpPost} function is used when the arguments
     * for the file system operation are passed in a HashMap, rather than a {@link ArgumentContainer} object.
     *
     * The {@link ArgumentContainer} is relatively new, and in general, all invocations will use that class for passing
     * arguments. But I am leaving this function here in case we end up wanting to use a HashMap for whatever reason.
     *
     * @param operationName The name of the file system operation to be performed.
     * @param functionUriBase The base URI of the serverless function. This tells the invoker where to issue the
     *                        HTTP POST request to.
     * @param nameNodeArguments Arguments for the NameNode itself. These would normally be passed in via the
     *                          commandline in traditional, serverful HopsFS.
     * @param fileSystemOperationArguments The arguments for the filesystem operation. Each key should directly
     *                                     correspond to the name of an argument, while the value should be the
     *                                     argument itself.
     * @return The response from the serverless NameNode.
     */
    @Override
    public JsonObject invokeNameNodeViaHttpPost(
        String operationName,
        String functionUriBase,
        HashMap<String, Object> nameNodeArguments,
        HashMap<String, Object> fileSystemOperationArguments) throws IOException, IllegalStateException
    {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // These are the arguments passed to the file system operation that we'd like to perform (e.g., create).
        JsonObject fileSystemOperationArgumentsJson = new JsonObject();

        // Populate the file system operation arguments JSON.
        if (fileSystemOperationArguments != null) {
            LOG.debug("Populating HTTP request with FS operation arguments now...");
            InvokerUtilities.populateJsonObjectWithArguments(
                    fileSystemOperationArguments, fileSystemOperationArgumentsJson);
            LOG.debug("Populated " + fileSystemOperationArgumentsJson.size() + " arguments.");
        }
        else {
            LOG.debug("No FS operation arguments specified.");
            fileSystemOperationArgumentsJson = new JsonObject();
        }

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        String requestId = UUID.randomUUID().toString();

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArgumentsJson,
                fileSystemOperationArgumentsJson, requestId);
    }

    /**
     * This performs all the logic. The public versions of this function accept parameters that are convenient
     * for the callers. They convert these parameters to a usable form, and then pass control off to this function.
     *
     * @param operationName The FS operation being performed.
     * @param functionUriBase The base URI of the serverless function. We issue an HTTP request to this URI
     *                        in order to invoke the function. Before the request is issued, a number is appended
     *                        to the end of the URI to target a particular serverless name node. After the number is
     *                        appended, we also append a string ?blocking=true to ensure that the operation blocks
     *                        until it is completed so that the result may be returned to the caller.
     * @param nameNodeArguments Arguments for the Name Node itself. These would traditionally be passed as command line
     *                          arguments when using a serverful name node.
     * @param fileSystemOperationArguments The parameters to the FS operation. Specifically, these are the arguments
     *                                     to the Java function which performs the FS operation.
     * @return The response from the Serverless NameNode.
     */
    private JsonObject invokeNameNodeViaHttpInternal(String operationName, String functionUriBase,
                                                     JsonObject nameNodeArguments,
                                                     JsonObject fileSystemOperationArguments,
                                                     String requestId) throws IOException, IllegalStateException {
        if (!hasBeenConfigured())
            throw new IllegalStateException("Invoker has not yet been configured.");

        StringBuilder builder = new StringBuilder();
        builder.append(functionUriBase);

        int functionNumber = -1;

        // Attempt to get the serverless function associated with the particular file/directory, if one exists.
        if (fileSystemOperationArguments != null &&
                fileSystemOperationArguments.has(ServerlessNameNodeKeys.SRC)) {
            String sourceFileOrDirectory =
                    (String) fileSystemOperationArguments.getAsJsonPrimitive("src").getAsString();
            LOG.debug("Checking serverless function cache for entry associated with file/directory \"" +
                    sourceFileOrDirectory + "\" now...");
            functionNumber = cache.getFunction(sourceFileOrDirectory);
        } else {
            LOG.debug("No `src` property found in file system arguments... skipping the checking of INode cache...");
        }

        // If we have a cache entry for this function, then we'll invoke that specific function.
        // Otherwise, we'll just select a function at random.
        if (functionNumber < 0) {
            functionNumber = ThreadLocalRandom.current().nextInt(0, numUniqueFunctions + 1);
            LOG.debug("Randomly selected serverless function " + functionNumber);
        } else {
            LOG.debug("Retrieved serverless function " + functionNumber + " from cache.");
        }

        builder.append(functionNumber);

        // Add the blocking parameter to the end of the URI so the function blocks until it completes
        // and the full result can be returned to the user.
        builder.append(blockingParameter);
        String functionUri = builder.toString();

        LOG.info(String.format("Preparing to invoke OpenWhisk serverless function with URI \"%s\" \nfor FS operation \"%s\" now...",
                functionUri, operationName));

        HttpPost request = new HttpPost(functionUri);

        // This is the top-level JSON object passed along with the HTTP POST request.
        JsonObject requestArguments = new JsonObject();

        // We pass the file system operation arguments to the NameNode, as it
        // will hand them off to the intended file system operation function.
        nameNodeArguments.add(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS, fileSystemOperationArguments);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.OPERATION, operationName);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.IS_CLIENT_INVOKER, isClientInvoker);

        addStandardArguments(nameNodeArguments, requestId);

        // OpenWhisk expects the arguments for the serverless function handler to be included in the JSON contained
        // within the HTTP POST request. They should be included with the key "value".
        requestArguments.add(ServerlessNameNodeKeys.VALUE, nameNodeArguments);

        // Prepare the HTTP POST request.
        StringEntity parameters = new StringEntity(requestArguments.toString());
        request.setEntity(parameters);
        request.setHeader("Content-type", "application/json");
        request.setHeader("Authorization", "Basic Basic 789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP");

        LOG.info("Invoking the OpenWhisk serverless NameNode function for operation " + operationName + " now...");

        LOG.debug("HttpRequest (before issuing it): " + request);
        LOG.debug("Request URI/URL: " + request.getURI().toURL());

        int currentNumTries = 0;

        while (currentNumTries < maxHttpRetries) {
            LOG.debug("Invoking NameNode (op=" + operationName + "), attempt " + (currentNumTries + 1)
                    + "/" + maxHttpRetries + ".");
            HttpResponse httpResponse = httpClient.execute(request);
            int responseCode = httpResponse.getStatusLine().getStatusCode();

            // If we receive a 4XX or 5XX response code, then we should re-try. HTTP 4XX errors
            // generally indicate a client error, but sometimes I receive this error right after
            // updating the NameNodes. OpenWhisk complains that the function hasn't been initialized
            // yet, but if you try again a few seconds later, then the request will get through.
            if (responseCode >= 400 && responseCode <= 599) {
                LOG.error("Received HTTP response code " + responseCode + " on attempt " +
                        (currentNumTries + 1) + "/" + maxHttpRetries + ".");

                if ((currentNumTries + 1) < maxHttpRetries) {
                    long sleepInterval = getExponentialBackoffInterval(currentNumTries);
                    LOG.debug("Sleeping for " + sleepInterval + " milliseconds before issuing another request...");
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                currentNumTries++;
                continue;
            }

            LOG.debug("Received HTTP response for request/task " + requestId + " (op=" + operationName + ").");
            return processHttpResponse(httpResponse);
        }

        throw new IOException("The file system operation could not be completed. " +
                "Failed to invoke a Serverless NameNode after " + maxHttpRetries + " attempts.");
    }

    /**
     * Process the HTTP response returned by the NameNode.
     *
     * @param httpResponse The response returned by the NameNode.
     * @return The result intended for the HopsFS client in the form of a JSON object.
     */
    private JsonObject processHttpResponse(HttpResponse httpResponse) throws IOException {
        String json = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        Gson gson = new Gson();

        int responseCode = httpResponse.getStatusLine().getStatusCode();
        String reasonPhrase = httpResponse.getStatusLine().getReasonPhrase();
        String protocolVersion = httpResponse.getStatusLine().getProtocolVersion().toString();

        Header contentType = httpResponse.getEntity().getContentType();
        long contentLength = httpResponse.getEntity().getContentLength();

        LOG.debug("====== HTTP RESPONSE ======");
        LOG.debug(protocolVersion + " - " + responseCode);
        LOG.debug(reasonPhrase);
        LOG.debug("---------------------------");
        LOG.debug(contentType.getName() + ": " + contentType.getValue());
        LOG.debug("Content-length: " + contentLength);
        LOG.debug("===========================");

        LOG.debug("HTTP Response from OpenWhisk function:\n" + httpResponse);
        LOG.debug("HTTP Response Entity: " + httpResponse.getEntity());
        LOG.debug("HTTP Response Entity Content: " + json);

        JsonObject jsonObjectResponse = null;
        JsonPrimitive jsonPrimitiveResponse = null;

        // If there was an OpenWhisk error, like a 502 Bad Gateway (for example), then this will
        // be a JsonPrimitive object. Specifically, it'd be a String containing the error message.
        try {
            jsonObjectResponse = gson.fromJson(json, JsonObject.class);
        } catch (JsonSyntaxException ex) {
            jsonPrimitiveResponse = gson.fromJson(json, JsonPrimitive.class);

            throw new IOException("Unexpected response from OpenWhisk function invocation: "
                    + jsonPrimitiveResponse.getAsString());
        }

        return jsonObjectResponse;
    }

    /**
     * Invoke a serverless NameNode function via HTTP POST, which is the standard/only way of "invoking" a serverless
     * function in Serverless HopsFS. (OpenWhisk supports HTTP GET, I think? But we don't use that, in any case.)
     *
     * This overload of the {@link ServerlessInvokerBase#invokeNameNodeViaHttpPost} function is used when the arguments
     * for a {@link ArgumentContainer} object.
     * @param operationName The name of the file system operation to be performed.
     * @param functionUriBase The base URI of the serverless function. This tells the invoker where to issue the
     *                        HTTP POST request to.
     * @param nameNodeArguments Arguments for the NameNode itself. These would normally be passed in via the
     *                          commandline in traditional, serverful HopsFS.
     * @param fileSystemOperationArguments The arguments for the filesystem operation.
     * @return The response from the serverless NameNode.
     */
    @Override
    public JsonObject invokeNameNodeViaHttpPost(String operationName, String functionUriBase,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments)
            throws IOException, IllegalStateException {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        String requestId = UUID.randomUUID().toString();

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArgumentsJson,
                fileSystemOperationArguments.convertToJsonObject(), requestId);
    }

    /**
     * Invoke a serverless NameNode function via HTTP POST, which is the standard/only way of "invoking" a serverless
     * function in Serverless HopsFS. (OpenWhisk supports HTTP GET, I think? But we don't use that, in any case.)
     *
     * This overload of the {@link ServerlessInvokerBase#invokeNameNodeViaHttpPost} function is used when the arguments
     * for a {@link ArgumentContainer} object AND when we are passing a specific requestId to the function.
     *
     * We pass specific requestIds when we want that function to have that requestId. This occurs when we are
     * concurrently issuing a TCP request with the HTTP request. We want both the TCP request and the HTTP request
     * to have the same requestID, since they both correspond to the same file system operation. The NameNode uses
     * the requestID to ensure it only completes that particular FS operation once.
     *
     * @param operationName The name of the file system operation to be performed.
     * @param functionUriBase The base URI of the serverless function. This tells the invoker where to issue the
     *                        HTTP POST request to.
     * @param nameNodeArguments Arguments for the NameNode itself. These would normally be passed in via the
     *                          commandline in traditional, serverful HopsFS.
     * @param fileSystemOperationArguments The arguments for the filesystem operation.
     * @param requestId The unique ID used to match this request uniquely against its corresponding TCP request.
     * @return The response from the serverless NameNode.
     */
    @Override
    public JsonObject invokeNameNodeViaHttpPost(String operationName, String functionUriBase,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments,
                                                String requestId)
            throws IOException, IllegalStateException {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArgumentsJson,
                fileSystemOperationArguments.convertToJsonObject(), requestId);
    }

    /**
     * Return an HTTP client configured appropriately for the OpenWhisk serverless platform.
     */
    @Override
    public CloseableHttpClient getHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        // We create the client in this way in order to avoid SSL certificate validation/verification errors.
        // The solution here is provided by:
        // https://gist.github.com/mingliangguo/c86e05a0f8a9019b281a63d151965ac7

        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {  }
                }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new SecureRandom());
        return HttpClients
            .custom()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .setSSLContext(sc)
            .build();
    }

    /**
     * Extract the result of a file system operation from the {@link JsonObject} returned by the NameNode.
     * @param response The response from the NameNode.
     * @return The result contained within the JsonObject returned by the NameNode.
     */
    @Override
    public Object extractResultFromJsonResponse(JsonObject response) {
        // First, let's check and see if there's any information about file/directory-to-function mappings.
        if (response.has(ServerlessNameNodeKeys.FUNCTION_MAPPING)) {
            LOG.debug("JSON response from serverless name node contains function mapping information.");
            JsonObject functionMapping = response.getAsJsonObject("FUNCTION_MAPPING");

            String src = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FILE_OR_DIR).getAsString();
            long parentINodeId = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.PARENT_ID).getAsLong();
            int function = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FUNCTION).getAsInt();

            LOG.debug("File or directory: \"" + src + "\", parent INode ID: " + parentINodeId +
                    ", function: " + function);

            cache.addEntry(src, function, false);

            LOG.debug("Added entry to function-mapping cache. File/directory \"" + src + "\" --> " + function);
        } else {
            LOG.warn("No INode function mapping information contained within response from serverless name node...");
        }

        // Print any exceptions that were encountered first.
        if (response.has(ServerlessNameNodeKeys.EXCEPTIONS)) {
            JsonArray exceptionsJson = response.get(ServerlessNameNodeKeys.EXCEPTIONS).getAsJsonArray();

            LOG.warn("The ServerlessNameNode encountered " + exceptionsJson.size()
                    + (exceptionsJson.size() == 1 ? " exception" : " exceptions") + ".");

            for (int i = 0; i < exceptionsJson.size(); i++)
                LOG.error(exceptionsJson.get(i).getAsString());
        }

        // Now we'll check for a result from the name node.
        if (response.has(ServerlessNameNodeKeys.RESULT)) {
            String resultBase64 = response.getAsJsonPrimitive(ServerlessNameNodeKeys.RESULT).getAsString();

            try {
                Object result = InvokerUtilities.base64StringToObject(resultBase64);

                if (result == null || (result instanceof NullResult)) {
                    return null;
                }

                LOG.debug("Returning object of type " + result.getClass().getSimpleName() + ": " + result);
                return result;
            } catch (Exception ex) {
                LOG.error("Error encountered while extracting result from NameNode response:", ex);
                return null;
            }
        }

        return null;
    }

    /**
     * Return the time to wait, in milliseconds, given the current number of attempts.
     * @param n The current number of attempts.
     * @return The time to wait, in milliseconds, before attempting another request.
     */
    private long getExponentialBackoffInterval(int n) {
        double interval = Math.pow(2, n);
        int jitter = random.nextInt( 1000);
        return (long)Math.min(interval + jitter, maxBackoffMilliseconds);
    }

    /**
     * Assign a name to this invoker to identify it. Mostly used for debugging (so we can tell who invoked what when
     * debugging the NameNodes).
     *
     * @param clientName the name of the client (e.g., the `clientName` field of the DFSClient class).
     */
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * Calls the `terminate()` function of the "INode mapping" cache. This is needed in order to close the
     * connection to the local Redis server. (The Redis server maintains the client-side INode cache mapping.)
     */
    @Override
    public void terminate() {
        cache.terminate();
    }
}
