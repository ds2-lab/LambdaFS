package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
 * Concrete implementation of the {@link ServerlessInvoker} interface for the OpenWhisk serverless platform.
 */
public class OpenWhiskInvoker extends ServerlessInvokerBase<JsonObject> {
    private static final Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * This is appended to the end of the serverlessEndpointBase AFTER the number is added.
     */
    private final String blockingParameter = "?blocking=true";

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
    public OpenWhiskInvoker() throws NoSuchAlgorithmException, KeyManagementException {
        super();
    }

    public OpenWhiskInvoker(Configuration conf) throws NoSuchAlgorithmException, KeyManagementException {
        super(conf);
    }

    @Override
    public JsonObject invokeNameNodeViaHttpPost(
        String operationName,
        String functionUriBase,
        HashMap<String, Object> nameNodeArguments,
        HashMap<String, Object> fileSystemOperationArguments) throws IOException
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
     * This performs all of the logic. The public versions of this function accept parameters that are convenient
     * for the callers. They convert these parameters to a usable form, and then pass control off to this function.
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
                                                     String requestId) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(functionUriBase);

        int functionNumber = -1;

        // Attempt to get the serverless function associated with the particular file/directory, if one exists.
        if (fileSystemOperationArguments != null && fileSystemOperationArguments.has("src")) {
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

        LOG.debug("invokeNameNodeViaHttpPost() function called for operation \"" + operationName
                + "\". Printing call stack now...");
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : elements) {
            LOG.debug("\tat " + element.getClassName() + "." + element.getMethodName() + "(" + element.getFileName() + ":" + element.getLineNumber() + ")");
        }

        LOG.info(String.format("Preparing to invoke OpenWhisk serverless function with URI \"%s\" \nfor FS operation \"%s\" now...",
                functionUri, operationName));

        HttpPost request = new HttpPost(functionUri);

        // This is the top-level JSON object passed along with the HTTP POST request.
        JsonObject requestArguments = new JsonObject();

        // We pass the file system operation arguments to the NameNode, as it
        // will hand them off to the intended file system operation function.
        nameNodeArguments.add("fsArgs", fileSystemOperationArguments);
        nameNodeArguments.addProperty("op", operationName);
        nameNodeArguments.addProperty("clientName", clientName);
        nameNodeArguments.addProperty("isClientInvoker", isClientInvoker);

        InvokerUtilities.addStandardArguments(nameNodeArguments, requestId);

        // OpenWhisk expects the arguments for the serverless function handler to be included in the JSON contained
        // within the HTTP POST request. They should be included with the key "value".
        requestArguments.add("value", nameNodeArguments);

        // Prepare the HTTP POST request.
        StringEntity parameters = new StringEntity(requestArguments.toString());
        request.setEntity(parameters);
        request.setHeader("Content-type", "application/json");
        request.setHeader("Authorization", "Basic Basic 789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP");

        LOG.info("Invoking the OpenWhisk serverless NameNode function for operation " + operationName + " now...");

        LOG.debug("HttpRequest (before issuing it): " + request.toString());
        LOG.debug("Request URI/URL: " + request.getURI().toURL());

        HttpResponse httpResponse = httpClient.execute(request);

        LOG.info("HTTP Response from OpenWhisk function:\n" + httpResponse.toString());
        LOG.info("response.getEntity() = " + httpResponse.getEntity());

        String json = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        Gson gson = new Gson();

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
    public JsonObject invokeNameNodeViaHttpPost(String operationName, String functionUri,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments) throws IOException {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        String requestId = UUID.randomUUID().toString();

        return invokeNameNodeViaHttpInternal(operationName, functionUri, nameNodeArgumentsJson,
                fileSystemOperationArguments.convertToJsonObject(), requestId);
    }

    @Override
    public JsonObject invokeNameNodeViaHttpPost(String operationName, String functionUri,
                                                HashMap<String, Object> nameNodeArguments,
                                                ArgumentContainer fileSystemOperationArguments,
                                                String requestId) throws IOException {
        // These are the arguments given to the {@link org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode}
        // object itself. That is, these are NOT the arguments for the particular file system operation that we
        // would like to perform (e.g., create, delete, append, etc.).
        JsonObject nameNodeArgumentsJson = new JsonObject();

        // Populate the NameNode arguments JSON with any additional arguments specified by the user.
        if (nameNodeArguments != null)
            InvokerUtilities.populateJsonObjectWithArguments(nameNodeArguments, nameNodeArgumentsJson);

        return invokeNameNodeViaHttpInternal(operationName, functionUri, nameNodeArgumentsJson,
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

    @Override
    public Object extractResultFromJsonResponse(JsonObject response) throws IOException, ClassNotFoundException {
        // First, let's check and see if there's any information about file/directory-to-function mappings.
        if (response.has("FUNCTION_MAPPING")) {
            LOG.debug("JSON response from serverless name node contains function mapping information.");
            JsonObject functionMapping = response.getAsJsonObject("FUNCTION_MAPPING");

            String src = functionMapping.getAsJsonPrimitive("fileOrDirectory").getAsString();
            long parentINodeId = functionMapping.getAsJsonPrimitive("parentId").getAsLong();
            int function = functionMapping.getAsJsonPrimitive("function").getAsInt();

            LOG.debug("File or directory: \"" + src + "\", parent INode ID: " + parentINodeId +
                    ", function: " + function);

            cache.addEntry(src, function, false);

            LOG.debug("Added entry to function-mapping cache. File/directory \"" + src + "\" --> " + function);
        } else {
            LOG.warn("No INode function mapping information contained within response from serverless name node...");
        }

        // Print any exceptions that were encountered first.
        if (response.has("EXCEPTIONS")) {
            JsonArray exceptionsJson = response.get("EXCEPTIONS").getAsJsonArray();

            LOG.warn("The ServerlessNameNode encountered " + exceptionsJson.size()
                    + (exceptionsJson.size() == 1 ? "exception" : "exceptions") + ".");

            for (int i = 0; i < exceptionsJson.size(); i++)
                LOG.error(exceptionsJson.get(i).getAsString());
        }

        // Now we'll check for a result from the name node.
        if (response.has("RESULT")) {
            // String resultBase64 = response.getAsJsonPrimitive("base64result").getAsString();
            String resultBase64 = response.getAsJsonPrimitive("RESULT").getAsString();

            Object result = InvokerUtilities.base64StringToObject(resultBase64);
            LOG.debug("Returning object of type " + result.getClass().getSimpleName() + ": " + result.toString());
            return result;
        }

        return null;

        // Now we'll check for a result from the name node.
        // If there's no result, then there may have been an exception, which we'd need to log.
        /*if (response.has("RESULT")) {
            JsonObject resultJson = response.getAsJsonObject("RESULT");

            // Now we need to check if there's an entry for the key "base64result".
            // If there is, great! That means there is a result returned by the Serverless NN.
            // We'll extract it, deserialize it, and return it to the caller.
            // If there is NOT an entry for the key "base64result", then there is no result. Return null.
            if (resultJson.has("base64result")) {
                String resultBase64 = resultJson.getAsJsonPrimitive("base64result").getAsString();

                Object result = InvokerUtilities.base64StringToObject(resultBase64);
                LOG.debug("Returning object of type " + result.getClass().getSimpleName() + ": " + result.toString());
                return result;
            }
            else {
                LOG.warn("Serverless function is returning null value to caller.");
                return null;
            }
        } else if (response.has("EXCEPTION")) {
            String exception = response.getAsJsonPrimitive("EXCEPTION").getAsString();
            LOG.error("Exception encountered during Serverless NameNode execution.");
            LOG.error(exception);
        }
        return null;*/
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * Calls the `terminate()` function of the "INode mapping" cache.
     */
    @Override
    public void terminate() {
        cache.terminate();
    }
}
