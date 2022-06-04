package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNodeList;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZooKeeperInvalidation;
import org.nustaq.serialization.FSTConfiguration;

/**
 * This class provides a number of utility functions common to all or many serverless invokers.
 */
public class InvokerUtilities {
    private static final Log LOG = LogFactory.getLog(InvokerUtilities.class);

    private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    /**
     * One easy and important optimization is to register classes which are serialized for sure in your application
     * at the FSTConfiguration object. This way FST can avoid writing classnames.
     */
    static {
        conf.registerClass(LocatedBlocks.class, TransactionEvent.class, TransactionAttempt.class, NamespaceInfo.class,
                LastBlockWithStatus.class, HdfsFileStatus.class, DirectoryListing.class, FsServerDefaults.class,
                ActiveServerlessNameNodeList.class, ActiveServerlessNameNode.class, StorageReport.class,
                DatanodeInfo.class, StorageReceivedDeletedBlocks.class, ExtendedBlock.class, ZooKeeperInvalidation.class);
    }

    /**
     * Returns true if the given object is an array.
     *
     * Source: https://stackoverflow.com/questions/2725533/how-to-see-if-an-object-is-an-array-without-using-reflection
     */
    public static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }

    /**
     * Convert the given Serializable object to a Base64-encoded String.
     * @param obj The object to encode as a Base64 String.
     * @return The Base64-encoded String of the given object.
     * @throws IOException May be thrown when Serializing the object.
     */
    public static String serializableToBase64String(Serializable obj) throws IOException {
        // Source: https://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream( baos );
//        oos.writeObject(obj);
//        oos.close();
//        return Base64.getEncoder().encodeToString(baos.toByteArray());
        byte[] objectBytes = conf.asByteArray(obj);
        return Base64.getEncoder().encodeToString(objectBytes);
    }

    /**
     * Decode and deserialize the given object. The parameter should be a base64-encoded byte[], and the byte[]
     * should be generated via standard Java serialization.
     * @param base64Encoded The object to decode and deserialize.
     * @return The decoded and deserialized object.
     */
    public static Object base64StringToObject(String base64Encoded) {
        byte[] resultSerialized = org.apache.commons.codec.binary.Base64.decodeBase64(base64Encoded);
        return conf.asObject(resultSerialized);
    }

    /**
     * Serialize an object.
     * @param obj the object to be serialized.
     * @return The object in serialized form as a byte[].
     */
    public static byte[] serializableToBytes(Serializable obj) {
        return conf.asByteArray(obj);
    }

    /**
     * Deserialize an object.
     * @param bytes The serialized object in the form of a byte[].
     * @return The deserialized object.
     */
    public static Object bytesToObject(byte[] bytes) {
        return conf.asObject(bytes);
    }

    /**
     * This function is used to add an array argument to the JsonObject containing the arguments for a particular
     * file system operation. This function will process a given array or Collection and add each element contained
     * therein to the destination JsonObject. This function will check the types of the objects within the Collection
     * or array. If necessary, the objects will be serialized and base64-encoded before being stored in the JsonObject.
     *
     * @param value The array or collection that is being serialized.
     * @param dest The JsonObject to store the array in.
     * @param key The name of the argument/parameter. This name comes from the argument name in the Serverless NameNode
     *            function that performs the desired FS operation.
     */
    public static void populateWithArray(String key, Object[] value, JsonObject dest) {
        List<Object> valueAsList = Arrays.asList(value);
        JsonArray arr = new JsonArray();

        // We want to check what type of array/list this is. If it is an array/list
        // of byte, we will simply convert the entire array/list to a base64 string.
        Class<?> clazz = value.getClass().getComponentType();

        if (LOG.isDebugEnabled()) LOG.debug("Serializing Collection/Array argument with component type: " + clazz.getSimpleName());

        if (String.class.isAssignableFrom(clazz))
            valueAsList.forEach(e -> arr.add(new JsonPrimitive((String) e)));
        else if (Number.class.isAssignableFrom(clazz))
            valueAsList.forEach(e -> arr.add(new JsonPrimitive((Number) e)));
        else if (Boolean.class.isAssignableFrom(clazz))
            valueAsList.forEach(e -> arr.add(new JsonPrimitive((Boolean) e)));
        else if (Character.class.isAssignableFrom(clazz))
            valueAsList.forEach(e -> arr.add(new JsonPrimitive((Character) e)));
        else if (Serializable.class.isAssignableFrom(clazz)) {
            valueAsList.forEach(e -> {
                try {
                    String base64Encoded = InvokerUtilities.serializableToBase64String((Serializable)e);
                    arr.add(base64Encoded);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            });
        }
        else {
            throw new IllegalArgumentException("Argument " + key + " is not of a valid type: "
                    + clazz.getSimpleName());
        }

        dest.add(key, arr);
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

            if (LOG.isDebugEnabled()) LOG.debug("Serializing argument with key \"" + key + "\" and type " + value.getClass().getSimpleName());

            if (value instanceof String)
                dest.addProperty(key, (String)value);
            else if (value instanceof Number)
                dest.addProperty(key, (Number)value);
            else if (value instanceof Boolean)
                dest.addProperty(key, (Boolean)value);
            else if (value instanceof Character)
                dest.addProperty(key, (Character)value);
            else if (value instanceof Map) {
                if (LOG.isDebugEnabled()) LOG.debug("Serializing Map argument now...");
                JsonObject innerMap = new JsonObject();
                populateJsonObjectWithArguments((Map<String, Object>) value, innerMap);
                dest.add(key, innerMap);
            }
            else if (value.getClass().isArray()) {
                populateWithArray(key, (Object[])value, dest);
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

    /**
     * This function gets the internal IP address of the current node.
     *
     * This is useful when we're running on a VM within GCP.
     */
    public static String getInternalIpAddress() throws SocketException, UnknownHostException {
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        }
    }
}
