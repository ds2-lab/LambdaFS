package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * This class provides a number of utility functions common to all or many serverless invokers.
 */
public class InvokerUtilities {
    private static final Log LOG = LogFactory.getLog(InvokerUtilities.class);

    /**
     * Returns true if the given object is an array.
     *
     * Source: https://stackoverflow.com/questions/2725533/how-to-see-if-an-object-is-an-array-without-using-reflection
     */
    public static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
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
    public static void addStandardArguments(JsonObject nameNodeArgumentsJson) {
        nameNodeArgumentsJson.addProperty("command-line-arguments", "-regular");
    }

    /**
     * Convert the given Serializable object to a Base64-encoded String.
     * @param obj The object to encode as a Base64 String.
     * @return The Base64-encoded String of the given object.
     * @throws IOException May be thrown when Serializing the object.
     */
    public static String serializableToBase64String(Serializable obj) throws IOException {
        // Source: https://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject(obj);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    /**
     * Decode and deserialize the given object. The parameter should be a base64-encoded byte[], and the byte[]
     * should be generated via standard Java serialization.
     * @param base64Encoded The object to decode and deserialize.
     * @return The decoded and deserialized object.
     */
    public static Object base64StringToObject(String base64Encoded) throws IOException, ClassNotFoundException {
        byte[] resultSerialized = org.apache.commons.codec.binary.Base64.decodeBase64(base64Encoded);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(resultSerialized);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }

    /**
     * Process the arguments passed in the given HashMap. Attempt to add them to the JsonObject.
     *
     * Throws an exception if one of the arguments is not a String, Number, Boolean, or Character.
     * @param arguments The HashMap of arguments to add to the JsonObject.
     * @param dest The JsonObject to which we are adding arguments.
     */
    public static void populateJsonObjectWithArguments(Map<String, Object> arguments, JsonObject dest) throws IOException {
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
                            String base64Encoded = InvokerUtilities.serializableToBase64String((Serializable)value);
                            arr.add(base64Encoded);
                        }
                        else
                            throw new IllegalArgumentException("Argument " + key + " is not of a valid type: "
                                    + obj.getClass().toString());
                    }
                }
            }
            else if (value instanceof Serializable) {
                String base64Encoded = InvokerUtilities.serializableToBase64String((Serializable)value);
                dest.addProperty(key, base64Encoded);
            }
            else if (value == null)
                LOG.warn("Value associated with key \"" + key + "\" is null.");
            else
                throw new IllegalArgumentException("Value associated with key \"" + key + "\" is not of a valid type: "
                        + value.getClass().toString());
        }
    }
}
