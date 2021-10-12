package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.cache.FunctionMetadataMap;
import org.apache.http.impl.client.CloseableHttpClient;

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

/**
 * Base class of serverless invokers. Defines some concrete state (i.e., instance variables) used by
 * all serverless invokers.
 *
 * We use this class to generically refer to a serverless invoker regardless of what platform is being used. This
 * keeps the code cleaner/simpler. When writing code to invoke NameNodes, we don't have to consider which serverless
 * platform is being used. Instead, we just refer to this class, and we use the {@link ServerlessInvokerFactory} class
 * to obtain a concrete subclass in which the function logic is implemented.
 *
 * @param <T> The type of object returned by invoking serverless functions, usually a JsonObject.
 */
public abstract class ServerlessInvokerBase<T> {
    private static final Log LOG = LogFactory.getLog(ServerlessInvokerBase.class);

    /**
     * Flag indicated whether the invoker has been configured.
     * The invoker must be configured via `setConfiguration` before it can be used.
     */
    protected boolean configured = false;

    /**
     * HTTPClient used to invoke serverless functions.
     */
    protected final CloseableHttpClient httpClient;

    /**
     * Maintains a mapping of files and directories to the serverless functions responsible for caching the
     * metadata of the file/directory in question.
     */
    protected FunctionMetadataMap cache;

    /**
     * The number of uniquely-deployed serverless functions available for use.
     */
    protected int numUniqueFunctions;

    /**
     * Indicates whether this Invoker instance is used by a client/user. When false, indicates that this
     * Invoker is useb by a DataNode.
     */
    protected boolean isClientInvoker;

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
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (GeneralSecurityException e) {
            LOG.error(e);
        }
    }

    /**
     * Default constructor.
     */
    protected ServerlessInvokerBase() throws NoSuchAlgorithmException, KeyManagementException {
        instantiateTrustManager();
        httpClient = getHttpClient();
    }

    /**
     * Set parameters of the invoker specified in the HopsFS configuration.
     */
    public void setConfiguration(Configuration conf) {
        LOG.debug("Configuring ServerlessInvokerBase now...");
        cache = new FunctionMetadataMap(conf);
        numUniqueFunctions = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS,
                DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);
        debugEnabledNdb = conf.getBoolean(DFSConfigKeys.NDB_DEBUG, DFSConfigKeys.NDB_DEBUG_DEFAULT);
        debugStringNdb = conf.get(DFSConfigKeys.NDB_DEBUG_STRING, DFSConfigKeys.NDB_DEBUG_STRING_DEFAULT);
        LOG.debug("NDB debug enabled: " + debugEnabledNdb);
        if (debugEnabledNdb)
            LOG.debug("NDB debug string: " + debugStringNdb);

        configured = true;
    }

    public FunctionMetadataMap getFunctionMetadataMap() {
        return cache;
    }

    public abstract T invokeNameNodeViaHttpPost(
            String operationName,
            String functionUriBase,
            HashMap<String, Object> nameNodeArguments,
            HashMap<String, Object> fileSystemOperationArguments)
            throws IOException, IllegalStateException;

    public abstract T invokeNameNodeViaHttpPost(String operationName, String functionUri,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments)
            throws IOException, IllegalStateException;

    /**
     * Issue an HTTP request to invoke the NameNode. This version accepts a requestId to use rather than
     * generating one itself.
     */
    public abstract T invokeNameNodeViaHttpPost(String operationName, String functionUri,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments,
                                                String requestId)
            throws IOException, IllegalStateException;


    public abstract CloseableHttpClient getHttpClient()
            throws NoSuchAlgorithmException, KeyManagementException;

    public abstract Object extractResultFromJsonResponse(JsonObject response)
            throws IOException, ClassNotFoundException;

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

        nameNodeArgumentsJson.addProperty(
                ServerlessNameNodeKeys.CLIENT_INTERNAL_IP, InvokerUtilities.getInternalIpAddress());
    }
}
