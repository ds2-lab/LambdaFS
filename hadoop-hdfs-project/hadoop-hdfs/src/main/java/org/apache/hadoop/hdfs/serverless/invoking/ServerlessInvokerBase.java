package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ArgumentContainer;
import org.apache.hadoop.hdfs.serverless.cache.FunctionMetadataMap;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Base class of serverless invokers. Defines some concrete state (i.e., instance variables) used by
 * all serverless invokers.
 *
 * @param <T> The type of object returned by invoking serverless functions, usually a JsonObject.
 */
public abstract class ServerlessInvokerBase<T> {
    private static final Log LOG = LogFactory.getLog(ServerlessInvokerBase.class);

    /**
     * HTTPClient used to invoke serverless functions.
     */
    protected final CloseableHttpClient httpClient;

    /**
     * Maintains a mapping of files and directories to the serverless functions responsible for caching the
     * metadata of the file/directory in question.
     */
    protected final FunctionMetadataMap cache;

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
     */
    protected String clientName;

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
    public ServerlessInvokerBase() throws NoSuchAlgorithmException, KeyManagementException {
        instantiateTrustManager();
        httpClient = getHttpClient();
        cache = new FunctionMetadataMap();

        LOG.warn("No configuration provided for OpenWhiskInvoker. Defaulting to " +
                DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT + " available serverless functions.");
        // If we're not given a Configuration object to use, then just assume the default...
        numUniqueFunctions = DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT;
    }

    public ServerlessInvokerBase(Configuration conf) throws NoSuchAlgorithmException, KeyManagementException {
        numUniqueFunctions = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS,
                DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);
        instantiateTrustManager();
        httpClient = getHttpClient();
        cache = new FunctionMetadataMap(conf);
    }

    public FunctionMetadataMap getFunctionMetadataMap() {
        return cache;
    }

    public abstract T invokeNameNodeViaHttpPost(
            String operationName,
            String functionUriBase,
            HashMap<String, Object> nameNodeArguments,
            HashMap<String, Object> fileSystemOperationArguments)
            throws IOException;

    public abstract T invokeNameNodeViaHttpPost(String operationName, String functionUri,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments)
            throws IOException;


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
}
