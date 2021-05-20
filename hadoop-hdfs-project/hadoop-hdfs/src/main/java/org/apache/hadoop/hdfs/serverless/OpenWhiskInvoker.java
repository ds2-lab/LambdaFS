package org.apache.hadoop.hdfs.serverless;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
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
    public OpenWhiskInvoker() {
        httpClient = HttpClientBuilder.create().build();
    }

    public JsonObject invokeNameNodeViaHttpPost(
        String operationName,
        String functionUri,
        HashMap<String, Object> nameNodeArguments,
        HashMap<String, Object> fileSystemOperationArguments) throws IOException
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

        LOG.debug("Invoking the OpenWhisk serverless NameNode function now...");

        HttpResponse response = httpClient.execute(request);

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
}
