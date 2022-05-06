package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.*;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.cache.FunctionMetadataMap;
import org.apache.hadoop.hdfs.serverless.operation.execution.NullResult;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.*;
import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;

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
 * The "main" API exposed by this class is the {@code invokeNameNodeViaHttpPost} function, which issues an HTTP POST
 * to the API Gateway of the serverless platform. (It doesn't have to be the API Gateway; that's just often what the
 * serverless platform component is. We issue a request to whatever we're supposed to in order to invoke functions. In
 * the case of OpenWhisk, that's the API Gateway component.)
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
     * Flag indicating whether we are running in 'local mode', which is only relevant on the client-side.
     * If true, then there's only one NameNode running within a Docker container on the same VM as the HopsFS client.
     *
     * This is only used for debugging/profiling/testing.
     */
    protected boolean localMode;

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
    protected boolean udpEnabled;

    /**
     * The TCP port that we ultimately bound to. See the comment in 'ServerlessNameNodeClient' for its
     * 'tcpServerPort' instance field for explanation as to why this field exists.
     */
    protected int tcpPort;

    protected int udpPort;

    /**
     * The timeout, in milliseconds, for an HTTP request to a NameNode. This specifically
     * is the timeout for a connection to be established with the server. Timed-out requests
     * will be retried according to the maxHttpRetries instance variable.
     */
    protected int httpTimeoutMilliseconds;

    /**
     * The log level argument to be passed to serverless functions.
     */
    protected String serverlessFunctionLogLevel = "DEBUG";

    /**
     * Passed to serverless functions. Determines whether they execute the consistency protocol.
     */
    protected boolean consistencyProtocolEnabled = true;

    /**
     * Turns off metric collection to save time, network transfer, and memory.
     */
    protected boolean benchmarkModeEnabled = false;

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

    protected JsonObject processHttpResponse(HttpResponse httpResponse) throws IOException {
        String json = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        Gson gson = new Gson();

//        int responseCode = httpResponse.getStatusLine().getStatusCode();
//        String reasonPhrase = httpResponse.getStatusLine().getReasonPhrase();
//        String protocolVersion = httpResponse.getStatusLine().getProtocolVersion().toString();
//
//        Header contentType = httpResponse.getEntity().getContentType();
//        long contentLength = httpResponse.getEntity().getContentLength();
//
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("====== HTTP RESPONSE ======");
//            LOG.debug(protocolVersion + " - " + responseCode);
//            LOG.debug(reasonPhrase);
//            LOG.debug("---------------------------");
//            if (contentType != null)
//                LOG.debug(contentType.getName() + ": " + contentType.getValue());
//            LOG.debug("Content-length: " + contentLength);
//            LOG.debug("===========================");
//
//            LOG.debug("HTTP Response from function:\n" + httpResponse);
////            LOG.debug("HTTP Response Entity: " + httpResponse.getEntity());
////            LOG.debug("HTTP Response Entity Content: " + json);
//        }

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
    private void instantiateTrustManagerOriginal() {
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
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (GeneralSecurityException e) {
            LOG.error(e);
        }
    }

    /**
     * Get the TCP port being used by this client's TCP server.
     * @return The TCP port being used by this client's TCP server.
     */
    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * Update the TCP port being used by this client's TCP server.
     * @param tcpPort The new value for the TCP port.
     */
    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /**
     * Update the UDP port being used by this client's UDP server.
     * @param udpPort The new value for the UDP port.
     */
    public void setUdpPort(int udpPort) {
        this.udpPort = udpPort;
    }

    /**
     * Default constructor.
     */
    protected ServerlessInvokerBase() {
        // instantiateTrustManager();
        statisticsPackages = new HashMap<>();
        transactionEvents = new HashMap<>();
    }

    /**
     * Set parameters of the invoker specified in the HopsFS configuration.
     */
    public void setConfiguration(Configuration conf, String invokerIdentity) {
        if (LOG.isDebugEnabled()) LOG.debug("Configuring ServerlessInvokerBase now...");
        cache = new FunctionMetadataMap(conf);
        localMode = conf.getBoolean(SERVERLESS_LOCAL_MODE, SERVERLESS_LOCAL_MODE_DEFAULT);
        maxHttpRetries = conf.getInt(DFSConfigKeys.SERVERLESS_HTTP_RETRY_MAX,
                DFSConfigKeys.SERVERLESS_HTTP_RETRY_MAX_DEFAULT);
        tcpEnabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED,
                DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);
        udpEnabled = conf.getBoolean(SERVERLESS_USE_UDP, SERVERLESS_USE_UDP_DEFAULT);
        httpTimeoutMilliseconds = conf.getInt(DFSConfigKeys.SERVERLESS_HTTP_TIMEOUT,
                DFSConfigKeys.SERVERLESS_HTTP_TIMEOUT_DEFAULT) * 1000; // Convert from seconds to milliseconds.

        if (this.localMode)
            numDeployments = 1;
        else
            numDeployments = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS, DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

        this.invokerIdentity = invokerIdentity;

        // NDB
        debugEnabledNdb = conf.getBoolean(DFSConfigKeys.NDB_DEBUG, DFSConfigKeys.NDB_DEBUG_DEFAULT);
        debugStringNdb = conf.get(DFSConfigKeys.NDB_DEBUG_STRING, DFSConfigKeys.NDB_DEBUG_STRING_DEFAULT);
        if (LOG.isDebugEnabled()) {
            LOG.debug("NDB debug enabled: " + debugEnabledNdb);
            LOG.debug("TCP Enabled: " + tcpEnabled);
            LOG.debug("UDP Enabled: " + udpEnabled);

            if (debugEnabledNdb) LOG.debug("NDB debug string: " + debugStringNdb);
        }

        try {
            httpClient = getHttpClient();
        } catch (NoSuchAlgorithmException | KeyManagementException | CertificateException | KeyStoreException e) {
            e.printStackTrace();
            return;
        }
        configured = true;
    }

    public void setConsistencyProtocolEnabled(boolean enabled) {
        this.consistencyProtocolEnabled = enabled;
    }

    public void setServerlessFunctionLogLevel(String logLevel) {
        this.serverlessFunctionLogLevel = logLevel;
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

    /**
     * Process the HTTP response returned by the NameNode.
     *
     * @param httpResponse The response returned by the NameNode.
     * @return The result intended for the HopsFS client in the form of a JSON object.
     */
    // protected abstract JsonObject processHttpResponse(HttpResponse httpResponse) throws IOException;

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
    protected JsonObject invokeNameNodeViaHttpInternal(String operationName, String functionUriBase,
                                                       JsonObject nameNodeArguments,
                                                       JsonObject fileSystemOperationArguments,
                                                       String requestId, int targetDeployment,
                                                       HttpPost request)
            throws IOException, IllegalStateException {
        long invokeStart = System.nanoTime();
        if (!hasBeenConfigured())
            throw new IllegalStateException("Serverless Invoker has not yet been configured! " +
                    "You must configure it by calling .setConfiguration(...) before using it.");

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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Invoking the OpenWhisk serverless NameNode function for operation " + operationName +
                    " now (requestID = " + requestId + ")");
            LOG.debug("Request URI/URL: " + request.getURI().toURL());
        }

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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Invoking NameNode " + targetDeployment + " (op=" + operationName +
                        ", requestID=" + requestId + "), attempt " + (exponentialBackoff.getNumberOfRetries() - 1) +
                        "/" + maxHttpRetries + ". Time elapsed: " + timeElapsed + " milliseconds.");
            } else {
                LOG.info("Invoking NameNode " + targetDeployment + " (op=" + operationName +
                        ", requestID=" + requestId + "), attempt " + (exponentialBackoff.getNumberOfRetries() - 1) +
                        "/" + maxHttpRetries + ".");
            }

            CloseableHttpResponse httpResponse = null;
            JsonObject processedResponse;
            try {
                httpResponse = httpClient.execute(request);
                currentTime = System.nanoTime();
                timeElapsed = (currentTime - invokeStart) / 1000000.0;
                int responseCode = httpResponse.getStatusLine().getStatusCode();

                if (LOG.isDebugEnabled())
                    LOG.debug("Received HTTP " + responseCode + " response for request/task " + requestId + " (op=" + operationName +"). Time elapsed: " + timeElapsed + " milliseconds.");

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
                currentTime = System.nanoTime();
                timeElapsed = (currentTime - invokeStart) / 1000000.0;
                LOG.error("Attempt " + (exponentialBackoff.getNumberOfRetries()) + " to invoke operation " +
                        requestId + " targeting deployment " + targetDeployment + " timed out. Time elapsed: " +
                        timeElapsed + " ms.");
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
            if (LOG.isDebugEnabled()) LOG.debug("Returning result to client after " + duration + " milliseconds.");
            return processedResponse;
        } while (backoffInterval != -1);

        throw new IOException("The file system operation could not be completed. " +
                "Failed to invoke Serverless NameNode " + targetDeployment + " after " + maxHttpRetries + " attempts.");
    }

    /**
     * Return an HTTP client that could theoretically be used for any platform. This client trusts all certificates,
     * so it is insecure.
     */
    public CloseableHttpClient getGenericTrustAllHttpClient() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting HTTP connection timeout to " + httpTimeoutMilliseconds + " milliseconds.");
            LOG.debug("Setting HTTP socket timeout to " + httpTimeoutMilliseconds + " milliseconds.");
        }

        RequestConfig requestConfig = RequestConfig
                .custom()
                .setConnectTimeout(httpTimeoutMilliseconds)
                .setSocketTimeout(httpTimeoutMilliseconds)
                .build();

        // This allows us to issue multiple HTTP requests at once, which may or may not be desirable/useful...
        // Like, I'm not sure if that's something we'll do within the same process/thread.

        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        //LOG.debug("getAcceptedIssuers() called!");
                        return new X509Certificate[0];
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        //LOG.debug("checkClientTrusted() called!");
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        //LOG.debug("checkServerTrusted() called!");
                    }
                }
        };

        TrustStrategy acceptingTrustStrategy = (x509Certificates, s) -> {
            //LOG.debug("Checking if certificates " + x509Certificates + " are trusted. String s: " + s + ".");
            return true;
        };
        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());

        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder
                .<ConnectionSocketFactory>create()
                .register("https", csf)
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .build();

        // Need to pass registry to this type of connection manager, as custom SSLContext is ignored.
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        connectionManager.setMaxTotal(25);
        connectionManager.setDefaultMaxPerRoute(25);

        return HttpClients
                .custom()
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .setSSLSocketFactory(csf)
                .setSSLContext(sslContext)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionManager)
                .build();
    }

    /**
     * Get an HTTP client configured for the particular serverless platform.
     */
    public abstract CloseableHttpClient getHttpClient()
            throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException;

    /**
     * Extract the result of a file system operation from the {@link JsonObject} returned by the NameNode.
     * @param resp The response from the NameNode.
     * @return The result contained within the JsonObject returned by the NameNode.
     */
    public Object extractResultFromJsonResponse(JsonObject resp) {
        JsonObject response = resp;

        // TODO: This might happen if all requests to a NN time out, in which case we should either handle
        //       it more cleanly and/or throw a more meaningful exception.
        if (response == null) {
            throw new IllegalStateException("Response from serverless NameNode was null.");
        }

        // Sometimes(?) the actual result is included in the "body" key.
        if (response.has("body"))
            response = resp.get("body").getAsJsonObject();

        String requestId;
        if (response.has(REQUEST_ID))
            requestId = response.getAsJsonPrimitive(REQUEST_ID).getAsString();
        else
            requestId = "N/A";

        // First, let's check and see if there's any information about file/directory-to-function mappings.
        if (response.has(ServerlessNameNodeKeys.DEPLOYMENT_MAPPING) && cache != null) {
            JsonObject functionMapping = response.getAsJsonObject(ServerlessNameNodeKeys.DEPLOYMENT_MAPPING);

            String src = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FILE_OR_DIR).getAsString();
            long parentINodeId = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.PARENT_ID).getAsLong();
            int function = functionMapping.getAsJsonPrimitive(ServerlessNameNodeKeys.FUNCTION).getAsInt();

            cache.addEntry(src, function, false);
        }

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
        }

        // This is just related to debugging/metrics. There are objects called "STATISTICS PACKAGES" that have
        // certain statistics about NN execution. We're just checking if there are any included here and, if so,
        // then we extract them.
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

        // Similar to "STATISTICS PACKAGES". See comment above.
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

                if (LOG.isTraceEnabled())
                    LOG.trace("Returning object of type " + result.getClass().getSimpleName() + ": "
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

    /**
     * There are some arguments that will be included every single time with the same values. This function
     * adds those arguments, thereby reducing the amount of boiler-plate code.
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
        if (!nameNodeArgumentsJson.has(COMMAND_LINE_ARGS));
            nameNodeArgumentsJson.addProperty(COMMAND_LINE_ARGS, "-regular");

        nameNodeArgumentsJson.addProperty(REQUEST_ID, requestId);
        nameNodeArgumentsJson.addProperty(DEBUG_NDB, debugEnabledNdb);
        nameNodeArgumentsJson.addProperty(DEBUG_STRING_NDB, debugStringNdb);
        nameNodeArgumentsJson.addProperty(TCP_PORT, tcpPort);
        nameNodeArgumentsJson.addProperty(UDP_PORT, udpPort);
        nameNodeArgumentsJson.addProperty(BENCHMARK_MODE, benchmarkModeEnabled);

        // If we aren't a client invoker (e.g., DataNode, other NameNode, etc.), then don't populate the internal IP field.
        nameNodeArgumentsJson.addProperty(CLIENT_INTERNAL_IP, (isClientInvoker ? InvokerUtilities.getInternalIpAddress() : "0.0.0.0"));

        nameNodeArgumentsJson.addProperty(TCP_ENABLED, tcpEnabled);
        nameNodeArgumentsJson.addProperty(UDP_ENABLED, udpEnabled);
        nameNodeArgumentsJson.addProperty(LOCAL_MODE, localMode);
        nameNodeArgumentsJson.addProperty(CONSISTENCY_PROTOCOL_ENABLED, consistencyProtocolEnabled);
        nameNodeArgumentsJson.addProperty(LOG_LEVEL, OpenWhiskHandler.getLogLevelIntFromString(serverlessFunctionLogLevel));
    }

    ///////////////////////
    // DEBUGGING/METRICS //
    ///////////////////////

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
