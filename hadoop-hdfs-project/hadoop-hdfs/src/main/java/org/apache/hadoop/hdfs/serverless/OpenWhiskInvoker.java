package org.apache.hadoop.hdfs.serverless;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

/**
 * Concrete implementation of the {@link ServerlessInvoker} interface for the OpenWhisk serverless platform.
 */
public class OpenWhiskInvoker implements ServerlessInvoker<JsonObject> {
    private static final Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * HTTPClient used to invoke serverless functions.
     */
    private final CloseableHttpClient httpClient;

    /**
     * Default constructor.
     */
    public OpenWhiskInvoker() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
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

        httpClient = getHttpClient();
    }

    public JsonObject invokeNameNodeViaHttpPost(
        String operationName,
        String functionUri,
        HashMap<String, Object> nameNodeArguments,
        HashMap<String, Object> fileSystemOperationArguments) throws IOException
    {
        LOG.info(String.format("Preparing to invoke OpenWhisk serverless function with URI \"%s\" \nfor FS operation \"%s\" now...",
                functionUri, operationName));

        HttpPost request = new HttpPost(functionUri);

        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // These are the arguments passed to the file system operation that we'd like to perform (e.g., create).
        JsonObject fileSystemOperationArgumentsJson = new JsonObject();

        // This is the top-level JSON object passed along with the HTTP POST request.
        JsonObject requestArguments = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        // Populate the file system operation arguments JSON.
        if (fileSystemOperationArguments != null)
            populateJsonObjectWithArguments(fileSystemOperationArguments, fileSystemOperationArgumentsJson);

        // We pass the file system operation arguments to the NameNode, as it
        // will hand them off to the intended file system operation function.
        nameNodeArgumentsJson.add("fsArgs", fileSystemOperationArgumentsJson);
        nameNodeArgumentsJson.addProperty("op", operationName);

        addStandardArguments(nameNodeArgumentsJson);

        // OpenWhisk expects the arguments for the serverless function handler to be included in the JSON contained
        // within the HTTP POST request. They should be included with the key "value".
        requestArguments.add("value", nameNodeArgumentsJson);

        // Prepare the HTTP POST request.
        StringEntity parameters = new StringEntity(requestArguments.toString());
        request.setEntity(parameters);
        request.setHeader("Content-type", "application/json");
        request.setHeader("Authorization", "Basic Basic 789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP");

        LOG.info("Invoking the OpenWhisk serverless NameNode function now...");

        LOG.debug("HttpRequest (before issuing it): " + request.toString());
        LOG.debug("Request URI/URL: " + request.getURI().toURL());

        HttpResponse response = httpClient.execute(request);

        LOG.info("HTTP Response from OpenWhisk function:\n" + response.toString());
        LOG.info("response.getEntity() = " + response.getEntity());

        String json = EntityUtils.toString(response.getEntity(), "UTF-8");
        Gson gson = new Gson();
        return gson.fromJson(json, JsonObject.class);
    }

    /**
     * Process the arguments passed in the given HashMap. Attempt to add them to the JsonObject.
     *
     * Throws an exception if one of the arguments is not a String, Number, Boolean, or Character.
     * @param arguments The HashMap of arguments to add to the JsonObject.
     * @param jsonObject The JsonObject to which we are adding arguments.
     */
    private void populateJsonObjectWithArguments(HashMap<String, Object> arguments, JsonObject jsonObject) {
        for (Map.Entry<String, Object> entry : arguments.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String)
                jsonObject.addProperty(key, (String)value);
            else if (value instanceof Number)
                jsonObject.addProperty(key, (Number)value);
            else if (value instanceof Boolean)
                jsonObject.addProperty(key, (Boolean)value);
            else if (value instanceof Character)
                jsonObject.addProperty(key, (Character)value);
            else
                throw new IllegalArgumentException(
                        "Argument " + key + " is not of a valid type: " + value.getClass().toString());
        }
    }

    /**
     * There are some arguments that will be included every single time with the same values. This function
     * adds those arguments.
     *
     * This is implemented as a separate function so as to provide a centralized place to modify these
     * consistent arguments.
     *
     * @param nameNodeArgumentsJson The arguments to be passed to the ServerlessNameNode itself.
     */
    private void addStandardArguments(JsonObject nameNodeArgumentsJson) {
        nameNodeArgumentsJson.addProperty("command-line-arguments", "-regular");
    }

    /**
     * Return an HTTP client configured appropriately for the OpenWhisk serverless platform.
     */
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
}
