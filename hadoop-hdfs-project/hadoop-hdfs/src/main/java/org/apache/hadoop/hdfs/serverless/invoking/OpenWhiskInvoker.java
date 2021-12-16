package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;

import static com.google.common.hash.Hashing.consistentHash;

/**
 * The serverless platform being used is specified in the configuration files for Serverless HopsFS. Currently, it
 * defaults to OpenWhisk. In order to obtain an invoker, you simply utilize the {@link ServerlessInvokerBase} class,
 * passing whatever platform is specified in the configuration. The factory will provide a concrete implementation
 * for the platform being used.
 *
 * Traditionally, a client would call {@link org.apache.hadoop.hdfs.DistributedFileSystem#create(Path)} (for example)
 * when they want to create a file. Under the covers, this call would propagate to the
 * {@link org.apache.hadoop.hdfs.DFSClient} class. Then an RPC call would occur between the DFSClient and a remote
 * NameNode. Now, instead of using RPC, we use an HTTP request. The recipient of the HTTP request is the OpenWhisk
 * API Gateway, which will route our request to a NameNode container. As such, we must include in the HTTP request
 * the name of the function we want the NameNode to execute along with the function's arguments. We also pass
 * various configuration parameters, debug information, etc. in the HTTP payload. The NameNode will execute the
 * function for us, then return a result via HTTP.
 */
public class OpenWhiskInvoker extends ServerlessInvokerBase<JsonObject> {
    private static final Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * This is appended to the end of the serverlessEndpointBase AFTER the number is added.
     */
    private final String blockingParameter = "?blocking=true";

    /**
     * Because invokers are generally created via the {@link ServerlessInvokerFactory} class, this constructor
     * will not be used directly.
     */
    protected OpenWhiskInvoker() throws NoSuchAlgorithmException, KeyManagementException {
        super();
    }

    /**
     * Redirect a received request to another NameNode. This is useful when a client issues a write request to
     * a deployment that is not authorized to perform writes on the target file/directory.
     *
     * @param operationName The FS operation being performed. This is passed to the NameNode so that it knows which of
     *                      its functions it should execute. This is sort of taking the place of the RPC mechanism,
     *                      where ordinarily you'd just invoke an RPC method.
     * @param functionUriBase The base URI of the serverless function. We issue an HTTP request to this URI
     *                        in order to invoke the function. Before the request is issued, a number is appended
     *                        to the end of the URI to target a particular serverless name node. After the number is
     *                        appended, we also append a string ?blocking=true to ensure that the operation blocks
     *                        until it is completed so that the result may be returned to the caller.
     * @param nameNodeArguments Arguments for the Name Node itself. These would traditionally be passed as command line
     *                          arguments when using a serverful name node. We generally don't need to pass anything
     *                          for this parameter.
     * @param fileSystemOperationArguments The parameters to the FS operation. Specifically, these are the arguments
     *                                     to the Java function which performs the FS operation. The NameNode will
     *                                     extract these after it sees what function it is supposed to execute. These
     *                                     would traditionally just be passed as arguments to the RPC call, but we
     *                                     aren't using RPC.
     * @param requestId The unique ID used to match this request uniquely against its corresponding TCP request. If
     *                  passed a null, then a random ID is generated.
     * @param targetDeployment Specify the deployment to target. Use -1 to use the cache or a random deployment if no
     *                         cache entry exists.
     * @return The response from the Serverless NameNode.
     */
    @Override
    public JsonObject redirectRequest(String operationName, String functionUriBase,
                                      JsonObject nameNodeArguments, JsonObject fileSystemOperationArguments,
                                      String requestId, int targetDeployment) throws IOException {
        // Just hand everything off to the internal HTTP invoke method.
        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArguments,
                fileSystemOperationArguments, requestId, targetDeployment);
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
     * @param requestId The unique ID used to match this request uniquely against its corresponding TCP request. If
     *                  passed a null, then a random ID is generated.
     * @param targetDeployment Specify the deployment to target. Use -1 to use the cache or a random deployment if no
     *                         cache entry exists.
     * @return The response from the Serverless NameNode.
     */
    private JsonObject invokeNameNodeViaHttpInternal(String operationName, String functionUriBase,
                                                     JsonObject nameNodeArguments,
                                                     JsonObject fileSystemOperationArguments,
                                                     String requestId, int targetDeployment)
            throws IOException, IllegalStateException {
        long invokeStart = System.nanoTime();
        if (!hasBeenConfigured())
            throw new IllegalStateException("Serverless Invoker has not yet been configured! " +
                    "You must configure it by calling .setConfiguration(...) before using it.");

        StringBuilder builder = new StringBuilder();
        builder.append(functionUriBase);

        if (targetDeployment == -1) {
            // Attempt to get the serverless function associated with the particular file/directory, if one exists.
            if (fileSystemOperationArguments != null && fileSystemOperationArguments.has(ServerlessNameNodeKeys.SRC)) {
                String sourceFileOrDirectory =
                        fileSystemOperationArguments.getAsJsonPrimitive("src").getAsString();
                targetDeployment = cache.getFunction(sourceFileOrDirectory);
//                targetDeployment = consistentHash(sourceFileOrDirectory.hashCode(), numUniqueFunctions);
//
//                LOG.debug("Hashed target path " + sourceFileOrDirectory
//                        + " to deployment " + targetDeployment + ".");
            } else {
                LOG.debug("No `src` property found in file system arguments... " +
                        "skipping the checking of INode cache...");
            }
        } else {
            LOG.debug("Explicitly targeting deployment #" + targetDeployment + ".");
        }

        // If we have a cache entry for this function, then we'll invoke that specific function.
        // Otherwise, we'll just select a function at random.
        if (targetDeployment < 0) {
            targetDeployment = ThreadLocalRandom.current().nextInt(0, numDeployments);
            LOG.debug("Randomly selected serverless function " + targetDeployment);
        }

        builder.append(targetDeployment);

        // Add the blocking parameter to the end of the URI so the function blocks until it completes
        // and the full result can be returned to the user.
        builder.append(blockingParameter);
        String functionUri = builder.toString();

       // LOG.info(String.format("Preparing to invoke OpenWhisk serverless function with URI \"%s\" \nfor FS operation \"%s\" now...",
       //         functionUri, operationName));

        HttpPost request = new HttpPost(functionUri);

        // This is the top-level JSON object passed along with the HTTP POST request.
        JsonObject requestArguments = new JsonObject();

        // We pass the file system operation arguments to the NameNode, as it
        // will hand them off to the intended file system operation function.
        nameNodeArguments.add(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS, fileSystemOperationArguments);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.OPERATION, operationName);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.IS_CLIENT_INVOKER, isClientInvoker);
        nameNodeArguments.addProperty(ServerlessNameNodeKeys.INVOKER_IDENTITY, invokerIdentity);

        addStandardArguments(nameNodeArguments, requestId);

        // OpenWhisk expects the arguments for the serverless function handler to be included in the JSON contained
        // within the HTTP POST request. They should be included with the key "value".
        requestArguments.add(ServerlessNameNodeKeys.VALUE, nameNodeArguments);

        // Prepare the HTTP POST request.
        StringEntity parameters = new StringEntity(requestArguments.toString());
        request.setEntity(parameters);
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        request.setHeader(HttpHeaders.AUTHORIZATION, "Basic 789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP");

        //LOG.info("Invoking the OpenWhisk serverless NameNode function for operation " + operationName + " now...");
        //LOG.debug("Request URI/URL: " + request.getURI().toURL());

        ExponentialBackOff exponentialBackoff = new ExponentialBackOff.Builder()
                .setMaximumRetries(maxHttpRetries)
                .setInitialIntervalMillis(1000)
                .setMaximumIntervalMillis(30000)
                .setMultiplier(2.25)
                .setRandomizationFactor(0.5)
                .build();

        long backoffInterval = exponentialBackoff.getBackOffInMillis();

        do {
            long currentTime = System.nanoTime();
            double timeElapsed = (currentTime - invokeStart) / 1000000.0;
            LOG.info("Invoking NameNode " + targetDeployment + " (op=" + operationName + "), attempt "
                    + (exponentialBackoff.getNumberOfRetries() - 1) + "/" + maxHttpRetries +
                    ". Time elapsed: " + timeElapsed + " milliseconds.");

            CloseableHttpResponse httpResponse = null;
            JsonObject processedResponse;
            try {
                httpResponse = httpClient.execute(request);
                currentTime = System.nanoTime();
                timeElapsed = (currentTime - invokeStart) / 1000000.0;
                LOG.debug("Received HTTP response for request/task " + requestId + " (op=" + operationName +
                        "). Time elapsed: " + timeElapsed + " milliseconds.");

                int responseCode = httpResponse.getStatusLine().getStatusCode();

                // If we receive a 4XX or 5XX response code, then we should re-try. HTTP 4XX errors
                // generally indicate a client error, but sometimes I receive this error right after
                // updating the NameNodes. OpenWhisk complains that the function hasn't been initialized
                // yet, but if you try again a few seconds later, then the request will get through.
                if (responseCode >= 400 && responseCode <= 599) {
                    LOG.error("Received HTTP response code " + responseCode + " on attempt " +
                            (exponentialBackoff.getNumberOfRetries()) + "/" + maxHttpRetries + ".");
                    LOG.warn("Sleeping for " + backoffInterval + " milliseconds before trying again...");
                    doSleep(backoffInterval);
                    backoffInterval = exponentialBackoff.getBackOffInMillis();
                    httpResponse.close();
                    continue;
                }

                processedResponse = processHttpResponse(httpResponse);
            } catch (NoHttpResponseException | SocketTimeoutException ex) {
                LOG.debug("Attempt " + (exponentialBackoff.getNumberOfRetries()) + " to invoke NameNode " +
                        functionUri + " timed out.");
                LOG.warn("Sleeping for " + backoffInterval + " milliseconds before trying again...");
                doSleep(backoffInterval);
                backoffInterval = exponentialBackoff.getBackOffInMillis();
                continue;
            } catch (IOException ex) {
                LOG.error("Encountered IOException while invoking NN via HTTP:", ex);
                LOG.warn("Sleeping for " + backoffInterval + " milliseconds before trying again...");
                doSleep(backoffInterval);
                backoffInterval = exponentialBackoff.getBackOffInMillis();
                continue;
            } catch (Exception ex) {
                LOG.error("Unexpected error encountered while invoking NN via HTTP:", ex);
                throw new IOException("The file system operation could not be completed. "
                    + "Encountered unexpected " + ex.getClass().getSimpleName() + " while invoking NN.");
            } finally {
                // Make the request reusable.
                request.releaseConnection();

                if (httpResponse != null)
                    httpResponse.close();
            }

            long invokeEnd = System.nanoTime();
            double duration = (invokeEnd - invokeStart) / 1000000.0;
            LOG.debug("Returning result to client after " + duration + " milliseconds.");
            return processedResponse;
        } while (backoffInterval != -1);

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
        if (contentType != null)
            LOG.debug(contentType.getName() + ": " + contentType.getValue());
        LOG.debug("Content-length: " + contentLength);
        LOG.debug("===========================");

        LOG.debug("HTTP Response from OpenWhisk function:\n" + httpResponse);
        LOG.debug("HTTP Response Entity: " + httpResponse.getEntity());
        // LOG.debug("HTTP Response Entity Content: " + json);

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
     * @param requestId The unique ID used to match this request uniquely against its corresponding TCP request. If
     *                  passed a null, then a random ID is generated.
     * @param targetDeployment Specify the deployment to target. Use -1 to use the cache or a random deployment if no
     *                         cache entry exists.
     * @return The response from the serverless NameNode.
     */
    @Override
    public JsonObject invokeNameNodeViaHttpPost(String operationName, String functionUriBase,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments,
                                                String requestId, int targetDeployment)
            throws IOException, IllegalStateException {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        if (requestId == null)
            requestId = UUID.randomUUID().toString();

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArgumentsJson,
                fileSystemOperationArguments.convertToJsonObject(), requestId, targetDeployment);
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

        LOG.debug("Setting HTTP connection timeout to " + httpTimeoutMilliseconds + " milliseconds.");
        LOG.debug("Setting HTTP socket timeout to " + httpTimeoutMilliseconds + " milliseconds.");

        RequestConfig requestConfig = RequestConfig
                .custom()
                .setConnectTimeout(httpTimeoutMilliseconds)
                .setSocketTimeout(httpTimeoutMilliseconds)
                .build();

        // This allows us to issue multiple HTTP requests at once, which may or may not be desirable/useful...
        // Like, I'm not sure if that's something we'll do within the same process/thread.
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(25);
        connectionManager.setDefaultMaxPerRoute(25);

        return HttpClients
            .custom()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .setSSLContext(sc)
            .setDefaultRequestConfig(requestConfig)
            .setConnectionManager(connectionManager)
            .build();
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
