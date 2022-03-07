package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_NUCLIO_ENDPOINTS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_NUCLIO_ENDPOINTS_DEFAULT;

public class NuclioInvoker extends ServerlessInvokerBase<JsonObject> {
    private static final Log LOG = LogFactory.getLog(NuclioInvoker.class);

    /**
     * With Nuclio, each function deployment is exposed by a LoadBalancer.
     * The endpoints of the load balancers are specified in the config. We store these endpoints in-memory in this map.
     */
    private final HashMap<Integer, String> deploymentToEndpointMap;

    /**
     * Because invokers are generally created via the {@link ServerlessInvokerFactory} class, this constructor
     * will not be used directly.
     */
    protected NuclioInvoker() throws NoSuchAlgorithmException, KeyManagementException {
        deploymentToEndpointMap = new HashMap<>();
    }

    @Override
    public void setConfiguration(Configuration conf, String invokerIdentity) {
        super.setConfiguration(conf, invokerIdentity);

        String[] endpoints = conf.getStrings(SERVERLESS_NUCLIO_ENDPOINTS, SERVERLESS_NUCLIO_ENDPOINTS_DEFAULT);
        LOG.debug("Found " + endpoints.length + " Nuclio endpoint(s) in configuration.");
        for (int i = 0; i < endpoints.length; i++) {
            LOG.debug("Nuclio deployment #1's endpoint: " + endpoints[i]);
            deploymentToEndpointMap.put(i, endpoints[i]);
        }
    }

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

        JsonObject fsArgs = fileSystemOperationArguments.convertToJsonObject();
        HttpPost request = new HttpPost(getFunctionUri(functionUriBase, targetDeployment, fsArgs));

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArgumentsJson,
                fsArgs, requestId, targetDeployment, request);
    }

    private String getFunctionUri(String functionUriBase, int targetDeployment, JsonObject fileSystemOperationArguments) {
        StringBuilder builder = new StringBuilder();

        if (!this.localMode) {
            if (targetDeployment == -1) {
                // Attempt to get the serverless function associated with the particular file/directory, if one exists.
                if (fileSystemOperationArguments != null && fileSystemOperationArguments.has(ServerlessNameNodeKeys.SRC)) {
                    String sourceFileOrDirectory =
                            fileSystemOperationArguments.getAsJsonPrimitive("src").getAsString();
                    targetDeployment = cache.getFunction(sourceFileOrDirectory);
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

            if (!deploymentToEndpointMap.containsKey(targetDeployment)) {
                throw new IllegalStateException("Nuclio deployment-to-endpoint mapping does not contain entry for target deployment " + targetDeployment);
            }

            builder.append("http://");
            builder.append(deploymentToEndpointMap.get(targetDeployment));
        }

        return builder.toString();
    }

    @Override
    protected JsonObject processHttpResponse(HttpResponse httpResponse) throws IOException {
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

    @Override
    public CloseableHttpClient getHttpClient() throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException {
        return getGenericTrustAllHttpClient();
    }

    @Override
    public JsonObject redirectRequest(String operationName, String functionUriBase, JsonObject nameNodeArguments,
                                      JsonObject fileSystemOperationArguments, String requestId, int targetDeployment)
            throws IOException {
        HttpPost request = new HttpPost(getFunctionUri(functionUriBase, targetDeployment, fileSystemOperationArguments));

        return invokeNameNodeViaHttpInternal(operationName, functionUriBase, nameNodeArguments,
                fileSystemOperationArguments, requestId, targetDeployment, request);
    }
}
