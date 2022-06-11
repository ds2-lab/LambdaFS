package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessHttpFuture;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.reactor.IOReactorException;

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

public class NuclioInvoker extends ServerlessInvokerBase {
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
    protected void sendEnqueuedRequests() {
        throw new NotImplementedException("This feature is not supported for Nuclio invokers.");
    }

    @Override
    public void setConfiguration(Configuration conf, String invokerIdentity, String functionUriBase) {
        super.setConfiguration(conf, invokerIdentity, functionUriBase);
        String[] endpoints = conf.getStrings(SERVERLESS_NUCLIO_ENDPOINTS, SERVERLESS_NUCLIO_ENDPOINTS_DEFAULT);
        if (LOG.isDebugEnabled()) LOG.debug("Found " + endpoints.length + " Nuclio endpoint(s) in configuration.");
        for (int i = 0; i < endpoints.length; i++) {
            if (LOG.isDebugEnabled()) LOG.debug("Nuclio deployment #1's endpoint: " + endpoints[i]);
            deploymentToEndpointMap.put(i, endpoints[i]);
        }
    }

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

        if (requestId == null) requestId = UUID.randomUUID().toString();

        JsonObject fsArgs = fileSystemOperationArguments.convertToJsonObject();
        HttpPost request = new HttpPost(getFunctionUri(targetDeployment, fsArgs));

        return enqueueHttpRequestInt(operationName, nameNodeArgumentsJson, fsArgs, requestId, targetDeployment);
    }

    private String getFunctionUri(int targetDeployment, JsonObject fileSystemOperationArguments) {
        StringBuilder builder = new StringBuilder();

        if (!this.localMode) {
            if (targetDeployment == -1) {
                // Attempt to get the serverless function associated with the particular file/directory, if one exists.
                if (fileSystemOperationArguments != null && fileSystemOperationArguments.has(ServerlessNameNodeKeys.SRC)) {
                    String sourceFileOrDirectory =
                            fileSystemOperationArguments.getAsJsonPrimitive("src").getAsString();
                    targetDeployment = cache.getFunction(sourceFileOrDirectory);
                } else {
                    if (LOG.isDebugEnabled()) LOG.debug("No `src` property found in file system arguments... " +
                            "skipping the checking of INode cache...");
                }
            } else {
                if (LOG.isDebugEnabled()) LOG.debug("Explicitly targeting deployment #" + targetDeployment + ".");
            }

            // If we have a cache entry for this function, then we'll invoke that specific function.
            // Otherwise, we'll just select a function at random.
            if (targetDeployment < 0) {
                targetDeployment = ThreadLocalRandom.current().nextInt(0, numDeployments);
                if (LOG.isDebugEnabled()) LOG.debug("Randomly selected serverless function " + targetDeployment);
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
    public CloseableHttpAsyncClient getHttpClient() throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException, IOReactorException {
        return getGenericTrustAllHttpClient();
    }
}