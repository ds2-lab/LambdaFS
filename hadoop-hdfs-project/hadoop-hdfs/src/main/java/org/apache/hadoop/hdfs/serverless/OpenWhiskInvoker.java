package org.apache.hadoop.hdfs.serverless;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;

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
        LOG.debug("invokeNameNodeViaHttpPost() function called for operation \"" + operationName
                + "\". Printing call stack now...");
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : elements) {
            LOG.debug("\tat " + element.getClassName() + "." + element.getMethodName() + "(" + element.getFileName() + ":" + element.getLineNumber() + ")");
        }

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
        if (fileSystemOperationArguments != null) {
            LOG.debug("Populating HTTP request with FS operation arguments now...");
            populateJsonObjectWithArguments(fileSystemOperationArguments, fileSystemOperationArgumentsJson);
            LOG.debug("Populated " + fileSystemOperationArgumentsJson.size() + " arguments.");
        }
        else {
            LOG.debug("No FS operation arguments specified.");
            fileSystemOperationArgumentsJson = new JsonObject();
        }

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

        LOG.info("Invoking the OpenWhisk serverless NameNode function for operation " + operationName + " now...");

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
     * Convert the given Serializable object to a Base64-encoded String.
     * @param obj The object to encode as a Base64 String.
     * @return The Base64-encoded String of the given object.
     * @throws IOException May be thrown when Serializing the object.
     */
    private static String serializableToBase64String(Serializable obj) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        ObjectWritable.writeObject(out, obj, obj.getClass(), null);
        byte[] objectBytes = out.getData();
        return org.apache.commons.codec.binary.Base64.encodeBase64String(objectBytes);
    }

    /**
     * Process the arguments passed in the given HashMap. Attempt to add them to the JsonObject.
     *
     * Throws an exception if one of the arguments is not a String, Number, Boolean, or Character.
     * @param arguments The HashMap of arguments to add to the JsonObject.
     * @param dest The JsonObject to which we are adding arguments.
     */
    private void populateJsonObjectWithArguments(Map<String, Object> arguments, JsonObject dest) throws IOException {
        for (Map.Entry<String, Object> entry : arguments.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String)
                dest.addProperty(key, (String)value);
            else if (value instanceof Number)
                dest.addProperty(key, (Number)value);
            else if (value instanceof Boolean)
                dest.addProperty(key, (Boolean)value);
            else if (value instanceof Character)
                dest.addProperty(key, (Character)value);
            else if (value instanceof Map) {
                JsonObject innerMap = new JsonObject();
                populateJsonObjectWithArguments((Map<String, Object>) value, innerMap);
                dest.add(key, innerMap);
            }
            else if (value instanceof Collection) {
                JsonArray arr = new JsonArray();

                // We want to check what type of array/list this is. If it is an array/list
                // of byte, we will simply convert the entire array/list to a base64 string.
                Class<?> clazz = value.getClass().getComponentType();

                if (clazz == Byte.class) {
                    Collection<?> valueAsCollection = (Collection<?>)value;
                    Byte[] valueAsByteArray = (Byte[])valueAsCollection.toArray();
                    String encoded = Base64.getEncoder().encodeToString(ArrayUtils.toPrimitive(valueAsByteArray));
                    dest.addProperty(key, encoded);
                }
                else { // Note an array or list of byte.
                    for (Object obj : (List<?>) value) {
                        if (obj instanceof String)
                            arr.add((String) obj);
                        else if (obj instanceof Number)
                            arr.add((Number) obj);
                        else if (obj instanceof Boolean)
                            arr.add((Boolean) obj);
                        else if (obj instanceof Character)
                            arr.add((Character) obj);
                        else if (value instanceof Serializable) {
                            String base64Encoded = serializableToBase64String((Serializable)value);
                            arr.add(base64Encoded);
                        }
                        else
                            throw new IllegalArgumentException("Argument " + key + " is not of a valid type: "
                                    + obj.getClass().toString());
                    }
                }
            }
            else if (value instanceof Serializable) {
                String base64Encoded = serializableToBase64String((Serializable)value);
                dest.addProperty(key, base64Encoded);
            }
            else if (value == null)
                LOG.warn("Value associated with key \"" + key + "\" is null.");
            else
                throw new IllegalArgumentException("Value associated with key \"" + key + "\" is not of a valid type: "
                        + value.getClass().toString());
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

    @Override
    public Object extractResultFromJsonResponse(JsonObject response) throws IOException, ClassNotFoundException {
        if (response.has("RESULT")) {
            String resultBase64 = response.getAsJsonObject("RESULT").getAsJsonPrimitive("base64result").getAsString();
            byte[] resultSerialized = org.apache.commons.codec.binary.Base64.decodeBase64(resultBase64);

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(resultSerialized);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            return objectInputStream.readObject();
        } else if (response.has("EXCEPTION")) {
            String exception = response.getAsJsonPrimitive("EXCEPTION").getAsString();
            LOG.error("Exception encountered during Serverless NameNode execution.");
            LOG.error(exception);
        }
        return null;
    }

    /**
     * Returns true if the given object is an array.
     *
     * Source: https://stackoverflow.com/questions/2725533/how-to-see-if-an-object-is-an-array-without-using-reflection
     */
    private static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }
}
