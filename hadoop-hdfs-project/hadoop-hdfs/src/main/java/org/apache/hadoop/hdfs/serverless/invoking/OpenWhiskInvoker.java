package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessHttpFuture;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.reactor.IOReactorException;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.io.*;
import java.security.*;
import java.util.*;

import static com.google.common.hash.Hashing.consistentHash;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.BATCH;
import static org.apache.hadoop.hdfs.serverless.invoking.ServerlessUtilities.getFunctionNumberForFileOrDirectory;

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
public class OpenWhiskInvoker extends ServerlessInvokerBase {
    private static final Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * This is appended to the end of the serverlessEndpointBase AFTER the number is added.
     */
    private static final String blockingParameter = "?blocking=true";

    /**
     * OpenWhisk uses an authorization string when issuing HTTP requests (and also when using the CLI).
     */
    private String authorizationString;

    private static OpenWhiskInvoker instance;

    public static synchronized OpenWhiskInvoker getInstance()
            throws NoSuchAlgorithmException, KeyManagementException {
        if (instance == null)
            instance = new OpenWhiskInvoker();

        return instance;
    }

    /**
     * Because invokers are generally created via the {@link ServerlessInvokerFactory} class, this constructor
     * will not be used directly.
     */
    protected OpenWhiskInvoker() {
        super();
    }

    @Override
    public synchronized void setConfiguration(Configuration conf, String invokerIdentity, String functionUriBase) {
        super.setConfiguration(conf, invokerIdentity, functionUriBase);

        authorizationString = conf.get(DFSConfigKeys.SERVERLESS_OPENWHISK_AUTH,
                DFSConfigKeys.SERVERLESS_OPENWHISK_AUTH_DEFAULT);
    }

    /**
     * Create the full function URI based on the base URI and target deployment. For OpenWhisk, the name of the
     * function is part of the URI. Thus, the deployment number must be appended to the function URI base to create
     * the full URI.
     *
     * @param functionUriBase The base URI. This is common to all functions, regardless of deployment number.
     * @param targetDeployment The deployment we will be targeting in our future HTTP request.
     * @param fileSystemOperationArguments The arguments that will be sent to the NN. We pass this as we want access
     *                                     to the 'SRC' argument, assuming it exists. We use this to determine the
     *                                     target deployment in the case that the {@code targetDeployment} parameter
     *                                     is -1.
     *
     *                                     Previously, if the target deployment was -1 and there is no SRC argument,
     *                                     then we just targeted a random deployment. But this would not work anymore.
     *                                     The target deployment parameter must be greater than -1 if there is no
     *                                     'SRC' argument contained within the `fileSystemOperationArguments` parameter.
     * @return The full URI.
     */
    private String getFunctionUri(String functionUriBase, int targetDeployment, JsonObject fileSystemOperationArguments) {
        StringBuilder builder = new StringBuilder();
        builder.append(functionUriBase);

        if (!this.localMode) {
            if (targetDeployment == -1) {
                // Attempt to get the serverless function associated with the particular file/directory, if one exists.
                if (fileSystemOperationArguments != null && fileSystemOperationArguments.has(ServerlessNameNodeKeys.SRC)) {
                    String sourceFileOrDirectory =
                            fileSystemOperationArguments.getAsJsonPrimitive("src").getAsString();
                    targetDeployment = getFunctionNumberForFileOrDirectory(sourceFileOrDirectory, numDeployments); // cache.getFunction(sourceFileOrDirectory);
                } else {
                    if (LOG.isDebugEnabled()) LOG.debug("No `src` property found in file system arguments... " + "skipping the checking of INode cache...");
                }
            } else {
                if (LOG.isDebugEnabled()) LOG.debug("Explicitly targeting deployment #" + targetDeployment + ".");
            }

            // If we have a cache entry for this function, then we'll invoke that specific function.
            // Otherwise, we'll just select a function at random.
            if (targetDeployment < 0) {
                throw new NotImplementedException("The target deployment should not be negative by this point.");

//                targetDeployment = ThreadLocalRandom.current().nextInt(0, numDeployments);
//                if (LOG.isDebugEnabled()) LOG.debug("Randomly selected serverless function " + targetDeployment);
            }

            builder.append(targetDeployment);

            // Add the blocking parameter to the end of the URI so the function blocks until it completes
            // and the full result can be returned to the user.
            builder.append(blockingParameter);
        }

        return builder.toString();
    }

    /**
     * Invoke a serverless NameNode function via HTTP POST, which is the standard/only way of "invoking" a serverless
     * function in Serverless HopsFS. (OpenWhisk supports HTTP GET, I think? But we don't use that, in any case.)
     *
     * This overload of the {@link ServerlessInvokerBase#enqueueHttpRequestInt} function is used when the arguments
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
    public ServerlessHttpFuture enqueueHttpRequest(String operationName, String functionUriBase,
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

        JsonObject fsArgs = fileSystemOperationArguments.convertToJsonObject();
        return enqueueHttpRequestInt(operationName, nameNodeArgumentsJson, fsArgs, requestId, targetDeployment);
    }

    @Override
    protected void sendEnqueuedRequests() throws UnsupportedEncodingException, SocketException, UnknownHostException {
        if (!hasBeenConfigured())
            throw new IllegalStateException("Serverless Invoker has not yet been configured! " +
                    "You must configure it by calling .setConfiguration(...) before using it.");

        List<List<JsonObject>> batchedRequests = new ArrayList<>();
        int totalNumRequestsBatched = createRequestBatches(batchedRequests);

        if (totalNumRequestsBatched == 0)
            return;

        // We've created all the batches. Now we need to issue the requests.
        int totalNumBatchedRequestsIssued = 0;
        for (int i = 0; i < numDeployments; i++) {
            List<JsonObject> deploymentBatches = batchedRequests.get(i);

            if (deploymentBatches.size() > 0) {
                if (LOG.isDebugEnabled()) LOG.debug("Preparing to send " + deploymentBatches.size() +
                        " batch(es) of requests to Deployment " + i);

                for (JsonObject requestBatch : deploymentBatches) {
                    // This is the top-level JSON object passed along with the HTTP POST request.
                    JsonObject topLevel = new JsonObject();

                    JsonObject nameNodeArguments = new JsonObject();
                    nameNodeArguments.add(BATCH, requestBatch);
                    addStandardArguments(nameNodeArguments);

                    // OpenWhisk expects the arguments for the serverless function handler to be included in the JSON contained
                    // within the HTTP POST request. They should be included with the key "value".
                    topLevel.add(ServerlessNameNodeKeys.VALUE, nameNodeArguments);

                    String requestUri = getFunctionUri(functionUriBase, i, topLevel);
                    HttpPost request = new HttpPost(requestUri);
                    request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authorizationString);

                    // Prepare the HTTP POST request.
                    StringEntity parameters = new StringEntity(topLevel.toString());
                    request.setEntity(parameters);
                    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

                    try {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Issuing batched HTTP request to deployment " + i +
                                    " with URI = " + requestUri + ", requestIDs = " +
                                    StringUtils.join(", ", requestBatch.keySet()));
                        }

                        doInvoke(request, topLevel, requestBatch.keySet());
                        totalNumBatchedRequestsIssued++;
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while issuing batched HTTP request:", ex);
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) LOG.debug("Issued a total of " + totalNumBatchedRequestsIssued + " batched HTTP request(s).");
    }

    /**
     * Return an HTTP client configured appropriately for the OpenWhisk serverless platform.
     */
    @Override
    public CloseableHttpAsyncClient getHttpClient() throws NoSuchAlgorithmException, KeyManagementException,
            CertificateException, KeyStoreException, IOReactorException {
        // We create the client in this way in order to avoid SSL certificate validation/verification errors.
        // The solution here is provided by:
        // https://gist.github.com/mingliangguo/c86e05a0f8a9019b281a63d151965ac7

        return getGenericTrustAllHttpClient();
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
        if (cache != null)
            cache.terminate();
    }
}
