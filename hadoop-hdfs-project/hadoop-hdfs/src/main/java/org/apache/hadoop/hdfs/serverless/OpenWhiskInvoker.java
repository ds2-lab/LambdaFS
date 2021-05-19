package org.apache.hadoop.hdfs.serverless;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.hdfs.UnknownCryptoProtocolVersionException;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Concrete implementation of the {@link ServerlessInvoker} interface for the OpenWhisk serverless platform.
 */
public class OpenWhiskInvoker implements ServerlessInvoker<JsonObject> {
    private static Log LOG = LogFactory.getLog(OpenWhiskInvoker.class);

    /**
     * HTTPClient used to invoke serverless functions.
     */
    private final CloseableHttpClient httpClient;

    public OpenWhiskInvoker() {
        httpClient = HttpClientBuilder.create().build();
    }

    public JsonObject invokeNameNodeViaHttpPost(
        String operationName,
        String functionUri,
        HashMap<String, String> nameNodeArguments,
        HashMap<String, String> fileSystemOperationArguments) throws IOException
    {
        LOG.debug(String.format("Preparing to invoke OpenWhisk serverless function with URI \"%s\" for FS operation \"%s\" now...",
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

        // Populate the NameNode arguments JSON.
        for (Map.Entry<String, String> entry : nameNodeArguments.entrySet())
            nameNodeArgumentsJson.addProperty(entry.getKey(), entry.getValue());

        // Populate the file system operation arguments JSON.
        for (Map.Entry<String, String> entry : fileSystemOperationArguments.entrySet())
            fileSystemOperationArgumentsJson.addProperty(entry.getKey(), entry.getValue());

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

        LOG.debug("Invoking the OpenWhisk serverless NameNode function now...");

        HttpResponse response = httpClient.execute(request);

        String json = EntityUtils.toString(response.getEntity(), "UTF-8");
        Gson gson = new Gson();
        return gson.fromJson(json, JsonObject.class);
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
}
