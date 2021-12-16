package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.cache.FunctionMetadataMap;
import org.apache.hadoop.hdfs.serverless.operation.NullResult;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_LOCAL_MODE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_LOCAL_MODE_DEFAULT;

/**
 * Base class of serverless invokers. Defines some concrete state (i.e., instance variables) used by
 * all serverless invokers.
 *
 * We use this class to generically refer to a serverless invoker regardless of what platform is being used. This
 * keeps the code cleaner/simpler. When writing code to invoke NameNodes, we don't have to consider which serverless
 * platform is being used. Instead, we just refer to this class, and we use the {@link ServerlessInvokerFactory} class
 * to obtain a concrete subclass in which the function logic is implemented.
 *
 * Traditionally, a client would call {@link org.apache.hadoop.hdfs.DistributedFileSystem#create(Path)} (for example)
 * when they want to create a file. Under the covers, this call would propagate to the
 * {@link org.apache.hadoop.hdfs.DFSClient} class. Then an RPC call would occur between the DFSClient and a remote
 * NameNode. Now, instead of using RPC, we use an HTTP request. The recipient of the HTTP request is the OpenWhisk
 * API Gateway, which will route our request to a NameNode container. As such, we must include in the HTTP request
 * the name of the function we want the NameNode to execute along with the function's arguments. We also pass
 * various configuration parameters, debug information, etc. in the HTTP payload. The NameNode will execute the
 * function for us, then return a result via HTTP.
 *
 * The DFSClient class used to maintain an RPC interface to a remote NameNode of type
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol}. I believe this was essentially just a reference to the
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer} instance running on the remote NameNode. To replace
 * this, I created a new class {@link ServerlessNameNodeClient} which implements the ClientProtocol interface. The
 * DFSClient class now calls methods on the ServerlessNameNodeClient object. Instead of being RPC methods, the
 * ServerlessNameNodeClient transparently issues HTTP/TCP requests to invoke NameNode functions. The
 * ServerlessNameNodeClient class interacts with a concrete child class of this ServerlessInvokerBase class to
 * invoke NameNode functions (e.g., the {@link OpenWhiskInvoker} class).
 *
 * @param <T> The type of object returned by invoking serverless functions, usually a JsonObject.
 */
public abstract class ServerlessInvokerBase<T> {
    private static final Log LOG = LogFactory.getLog(ServerlessInvokerBase.class);

    protected Registry<ConnectionSocketFactory> registry;

    /**
     * Store the statistics packages from serverless name nodes in this HashMap.
     *
     * Map from request ID to statistics packages.
     */
    protected HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statisticsPackages;

    /**
     * Store the transaction events from serverless name nodes in this HashMap.
     *
     * Map from request ID to statistics packages.
     */
    protected HashMap<String, List<TransactionEvent>> transactionEvents;

    /**
     * The maximum amount of time to wait before issuing another HTTP request after the previous request failed.
     *
     * TODO: Make this configurable.
     */
    private static final int maxBackoffMilliseconds = 5000;

    /**
     * Flag indicated whether the invoker has been configured.
     * The invoker must be configured via `setConfiguration` before it can be used.
     */
    protected boolean configured = false;

    /**
     * HTTPClient used to invoke serverless functions.
     */
    protected CloseableHttpClient httpClient;

    /**
     * Maintains a mapping of files and directories to the serverless functions responsible for caching the
     * metadata of the file/directory in question.
     */
    protected FunctionMetadataMap cache;

    /**
     * The number of uniquely-deployed serverless functions available for use.
     */
    protected int numDeployments;

    /**
     * Indicates whether this Invoker instance is used by a client/user. When false, indicates that this
     * Invoker is useb by a DataNode.
     */
    protected boolean isClientInvoker;

    /**
     * Name of the entity using this invoker.
     *
     * For DataNodes, this is DN-[the DataNode's transfer IP]:[the DataNode's transfer port].
     * For Clients, this is C-[the client's ID].
     * For NameNodes, this is NN[DeploymentNumber]-[the NameNode's ID].
     */
    protected String invokerIdentity;

    /**
     * Unique identifier of the particular client using this class.
     *
     * This name will be set automatically if a client/user is invoking. Otherwise, we default to DataNode.
     */
    protected String clientName = "DataNode";

    /**
     * Indicates whether we're being executed in a local container for testing/profiling/debugging purposes.
     */
    private boolean localMode;

    /**
     * If true, then we'll pass an argument to the NNs indicating that they should print their
     * debug output from the underlying NDB C++ library (libndbclient.so).
     */
    protected boolean debugEnabledNdb = false;

    /**
     * This string is passed to the NDB C++ library (on the NameNodes) if NDB debugging is enabled.
     */
    protected String debugStringNdb = null;

    /**
     * Maximum number of retries to be attempted for failed HTTP requests (response code in range [400, 599]).
     */
    protected int maxHttpRetries = DFSConfigKeys.SERVERLESS_HTTP_RETRY_MAX_DEFAULT;

    /**
     * If True, then we'll tell the NameNodes how to connect to this client's TCP server.
     * If False, then we'll tell the NameNodes not to try connecting to the TCP server.
     */
    protected boolean tcpEnabled;

    /**
     * The TCP port that we ultimately bound to. See the comment in 'ServerlessNameNodeClient' for its
     * 'tcpServerPort' instance field for explanation as to why this field exists.
     */
    protected int tcpPort;

    /**
     * The timeout, in milliseconds, for an HTTP request to a NameNode. This specifically
     * is the timeout for a connection to be established with the server. Timed-out requests
     * will be retried according to the maxHttpRetries instance variable.
     */
    protected int httpTimeoutMilliseconds;

    /**
     * Return the INode-NN mapping cache entry for the given file or directory.
     *
     * This function returns -1 if no such entry exists.
     * @param fileOrDirectory The file or directory in question.
     * @return The number of the NN to which the file or directory is mapped, if an entry exists in the cache. If no
     * entry exists, then -1 is returned.
     */
    public int getFunctionNumberForFileOrDirectory(String fileOrDirectory) {
        if (cache.containsEntry(fileOrDirectory))
            return cache.getFunction(fileOrDirectory);

        return -1;
    }

    /**
     * Create a TrustManager that does not validate certificate chains.
     *
     * This is necessary because otherwise, we may encounter SSL certificate errors when invoking serverless functions.
     * I do not know all the details behind this, but this resolves the errors.
     *
     * In the future, we may want to add certificate support for increased security, with a fall-back to this being an
     * option if the users do not want to configure SSL certificates.
     *
     * Per the Java documentation, TrustManagers are responsible for managing the trust material that is used when
     * making trust decisions, and for deciding whether credentials presented by a peer should be accepted.
     */
    private void instantiateTrustManager() {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        try {
            SSLContext sc = SSLContexts.custom().loadTrustMaterial(new TrustSelfSignedStrategy()).build();
            SSLConnectionSocketFactory socketFactory
                    = new SSLConnectionSocketFactory(sc);
            this.registry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", socketFactory)
                            .build();
            //sc.init(null, trustAllCerts, new java.security.SecureRandom());
            //HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (GeneralSecurityException e) {
            LOG.error(e);
        }
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /**
     * Default constructor.
     */
    protected ServerlessInvokerBase() {
        instantiateTrustManager();
        statisticsPackages = new HashMap<>();
        transactionEvents = new HashMap<>();
    }

    /**
     * Set parameters of the invoker specified in the HopsFS configuration.
     */
    public void setConfiguration(Configuration conf, String invokerIdentity) {
        LOG.debug("Configuring ServerlessInvokerBase now...");
        cache = new FunctionMetadataMap(conf);
        localMode = conf.getBoolean(SERVERLESS_LOCAL_MODE, SERVERLESS_LOCAL_MODE_DEFAULT);
        maxHttpRetries = conf.getInt(DFSConfigKeys.SERVERLESS_HTTP_RETRY_MAX,
                DFSConfigKeys.SERVERLESS_HTTP_RETRY_MAX_DEFAULT);
        tcpEnabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED,
                DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);
        httpTimeoutMilliseconds = conf.getInt(DFSConfigKeys.SERVERLESS_HTTP_TIMEOUT,
                DFSConfigKeys.SERVERLESS_HTTP_TIMEOUT_DEFAULT) * 1000; // Convert from seconds to milliseconds.

        if (localMode)
            numDeployments = 1;
        else
            numDeployments = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS, DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

        this.invokerIdentity = invokerIdentity;

        // NDB
        debugEnabledNdb = conf.getBoolean(DFSConfigKeys.NDB_DEBUG, DFSConfigKeys.NDB_DEBUG_DEFAULT);
        debugStringNdb = conf.get(DFSConfigKeys.NDB_DEBUG_STRING, DFSConfigKeys.NDB_DEBUG_STRING_DEFAULT);
        LOG.debug("NDB debug enabled: " + debugEnabledNdb);
        if (debugEnabledNdb)
            LOG.debug("NDB debug string: " + debugStringNdb);

        try {
            httpClient = getHttpClient();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
            return;
        }
        configured = true;
    }

    public FunctionMetadataMap getFunctionMetadataMap() {
        return cache;
    }

    /**
     * This performs all the logic. The public versions of this function accept parameters that are convenient
     * for the callers. They convert these parameters to a usable form, and then pass control off to this function.
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
    public abstract T invokeNameNodeViaHttpPost(String operationName, String functionUriBase,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments,
                                                String requestId, int targetDeployment)
            throws IOException, IllegalStateException;
    
    public abstract CloseableHttpClient getHttpClient()
            throws NoSuchAlgorithmException, KeyManagementException;

    /**
     * Extract the result of a file system operation from the {@link JsonObject} returned by the NameNode.
     * @param resp The response from the NameNode.
     * @return The result contained within the JsonObject returned by the NameNode.
     */
    public Object extractResultFromJsonResponse(JsonObject resp) {
        JsonObject response = resp;
        // Sometimes(?) the actual result is included in the "body" key.
        if (response.has("body"))
            response = resp.get("body").getAsJsonObject();

        String requestId;
        if (response.has(ServerlessNameNodeKeys.REQUEST_ID))
            requestId = response.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_ID).getAsString();
        else
            requestId = "N/A";

        // First, let's check and see if there's any information about file/directory-to-function mappings.
        if (response.has(ServerlessNameNodeKeys.DEPLOYMENT_MAPPING) && cache != null) {
            LOG.debug("JSON response from serverless name node contains function mapping information.");
            JsonObject functionMapping = response.getAsJsonObject(ServerlessNameNodeKeys.DEPLOYMENT_MAPPING);

            String src = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FILE_OR_DIR).getAsString();
            long parentINodeId = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.PARENT_ID).getAsLong();
            int function = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FUNCTION).getAsInt();

            LOG.debug("File or directory: \"" + src + "\", parent INode ID: " + parentINodeId +
                    ", function: " + function);

            cache.addEntry(src, function, false);

            LOG.debug("Added entry to function-mapping cache. File/directory \"" + src + "\" --> " + function);
        } /*else {
            LOG.warn("No INode function mapping information contained within response from serverless name node...");
        }*/

        // Print any exceptions that were encountered first.
        if (response.has(ServerlessNameNodeKeys.EXCEPTIONS)) {
            JsonArray exceptionsJson = response.get(ServerlessNameNodeKeys.EXCEPTIONS).getAsJsonArray();

            int numExceptions = exceptionsJson.size();

            if (numExceptions > 0) {
                LOG.warn("=+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=");
                LOG.warn("*** The ServerlessNameNode encountered " + numExceptions +
                        (numExceptions == 1 ? " exception. ***" : " exceptions. ***"));

                for (int i = 0; i < exceptionsJson.size(); i++)
                    LOG.error(exceptionsJson.get(i).getAsString());
                LOG.warn("=+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=");
            }
            else {
                LOG.debug("The Serverless NameNode did not encounter any exceptions while executing task " + requestId);
            }
        }

        if (response.has(ServerlessNameNodeKeys.STATISTICS_PACKAGE)) {
            String statisticsPackageEncoded =
                    response.getAsJsonPrimitive(ServerlessNameNodeKeys.STATISTICS_PACKAGE).getAsString();

            try {
                TransactionsStats.ServerlessStatisticsPackage statisticsPackage =
                        (TransactionsStats.ServerlessStatisticsPackage)
                                InvokerUtilities.base64StringToObject(statisticsPackageEncoded);
                this.statisticsPackages.put(requestId, statisticsPackage);
            } catch (Exception ex) {
                LOG.error("Error encountered while extracting statistics packages from NameNode response:", ex);
                return null;
            }
        }

        if (response.has(ServerlessNameNodeKeys.TRANSACTION_EVENTS)) {
            String transactionEventsEncoded =
                    response.getAsJsonPrimitive(ServerlessNameNodeKeys.TRANSACTION_EVENTS).getAsString();

            try {
                List<TransactionEvent> txEvents =
                        (List<TransactionEvent>) InvokerUtilities.base64StringToObject(transactionEventsEncoded);
                this.transactionEvents.put(requestId, txEvents);
            } catch (Exception ex) {
                LOG.error("Error encountered while extracting transaction events from NameNode response:", ex);
                return null;
            }
        }

        // Now we'll check for a result from the name node.
        if (response.has(ServerlessNameNodeKeys.RESULT)) {
            String resultBase64 = response.getAsJsonPrimitive(ServerlessNameNodeKeys.RESULT).getAsString();

            try {
                Object result = InvokerUtilities.base64StringToObject(resultBase64);

                if (result == null || (result instanceof NullResult)) {
                    return null;
                }

                LOG.debug("Returning object of type " + result.getClass().getSimpleName() + ": "
                        + result.toString());
                return result;
            } catch (Exception ex) {
                LOG.error("Error encountered while extracting result from NameNode response:", ex);
                return null;
            }
        }

        return null;
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
    public abstract T redirectRequest(String operationName, String functionUriBase,
                                      JsonObject nameNodeArguments, JsonObject fileSystemOperationArguments,
                                      String requestId, int targetDeployment) throws IOException;

    /**
     * Set the name of the client using this invoker (e.g., the `clientName` field of the DFSClient class).
     * @param clientName the name of the client (e.g., the `clientName` field of the DFSClient class).
     */
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * Calls the `terminate()` function of the "INode mapping" cache.
     */
    public void terminate() {
        cache.terminate();
    }

    /**
     * Sets the flag indicating whether this Invoker is used by a client/user.
     *
     * True indicates client/user, false indicates datanode.
     */
    public void setIsClientInvoker(boolean isClientInvoker) {
        this.isClientInvoker = isClientInvoker;
    }

    /**
     * Returns True if this invoker has already been configured via the `setConfiguration()` API.
     * Otherwise, returns false.
     */
    public boolean hasBeenConfigured() {
        return configured;
    }

//    /**
//     * Perform the sleep associated with exponential backoff.
//     *
//     * This checks the value of currentNumTries and compares it against maxHttpRetries.
//     * If we're out of tries, then we do not bother sleeping. Instead, we just return immediately.
//     *
//     * @param currentNumTries How many times we've attempted a request thus far.
//     */
//    protected void doExponentialBackoff(int currentNumTries) {
//        // Only bother sleeping (exponential backoff) if we're going to try at least one more time.
//        if ((currentNumTries + 1) > maxHttpRetries)
//            return;
//
//        long sleepInterval = getExponentialBackoffInterval(currentNumTries);
//        LOG.debug("Sleeping for " + sleepInterval + " milliseconds before issuing another request...");
//        try {
//            Thread.sleep(sleepInterval);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Hide try-catch for Thread.sleep().
     * @param backoffInterval How long to sleep in milliseconds.
     */
    protected void doSleep(long backoffInterval) {
        try {
            Thread.sleep(backoffInterval);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

//    /**
//     * Return the time to wait, in milliseconds, given the current number of attempts.
//     * @param n The current number of attempts.
//     * @return The time to wait, in milliseconds, before attempting another request.
//     */
//    private long getExponentialBackoffInterval(int n) {
//        double interval = Math.pow(2, n);
//        int jitter = random.nextInt( 1000);
//        return (long)Math.min(interval + jitter, maxBackoffMilliseconds);
//    }

    /**
     * There are some arguments that will be included every single time with the same values. This function
     * adds those arguments.
     *
     * This is implemented as a separate function to provide a centralized place to modify these
     * consistent arguments.
     *
     * The standard arguments are currently:
     *  - Command-line arguments for the NN (if none are provided already)
     *  - Request ID
     *  - Internal IP address of the node on which the client is running.
     *  - Flag indicating whether NDB debug logging should be enabled.
     *  - The NDB debug string that controls the format/content of the NDB debug logging.
     *  - Flag indicating whether TCP is enabled.
     *  - The TCP port that our TCP server is listening on.
     *  - Our internal IP address.
     *  - Flag indicating whether the NameNode is running in local mode.
     *
     * @param nameNodeArgumentsJson The arguments to be passed to the ServerlessNameNode itself.
     * @param requestId The request ID to use for this request. This will be added to the arguments.
     */
    protected void addStandardArguments(JsonObject nameNodeArgumentsJson, String requestId)
            throws SocketException, UnknownHostException {
        // If there is not already a command-line-arguments entry, then we'll add one with the "-regular" flag
        // so that the name node performs standard execution. If there already is such an entry, then we do
        // not want to overwrite it.
        if (!nameNodeArgumentsJson.has(ServerlessNameNodeKeys.COMMAND_LINE_ARGS))
            nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.COMMAND_LINE_ARGS, "-regular");

        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.REQUEST_ID, requestId);

        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.DEBUG_NDB, debugEnabledNdb);
        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.DEBUG_STRING_NDB, debugStringNdb);

        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.TCP_PORT, tcpPort);

        nameNodeArgumentsJson.addProperty(
                ServerlessNameNodeKeys.CLIENT_INTERNAL_IP, InvokerUtilities.getInternalIpAddress());
        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.TCP_ENABLED, tcpEnabled);
        nameNodeArgumentsJson.addProperty(ServerlessNameNodeKeys.LOCAL_MODE, localMode);
    }

    public HashMap<String, TransactionsStats.ServerlessStatisticsPackage> getStatisticsPackages() {
        return statisticsPackages;
    }

    public void setStatisticsPackages(HashMap<String, TransactionsStats.ServerlessStatisticsPackage> packages) {
        this.statisticsPackages = packages;
    }

    public HashMap<String, List<TransactionEvent>> getTransactionEvents() {
        return transactionEvents;
    }

    public void setTransactionEvents(HashMap<String, List<TransactionEvent>> transactionEvents) {
        this.transactionEvents = transactionEvents;
    }
}
