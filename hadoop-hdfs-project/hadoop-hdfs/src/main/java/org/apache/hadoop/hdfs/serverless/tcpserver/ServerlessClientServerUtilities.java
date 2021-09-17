package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryo.Kryo;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.logging.LogFactory;

/**
 * Utility functions exposed by both TCP clients and servers.
 */
public class ServerlessClientServerUtilities {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ServerlessClientServerUtilities.class);

    /**
     * This operation is used when a NameNode is first connecting to and registering with a client.
     */
    public static final String OPERATION_REGISTER = "REGISTER";

    /**
     * This operation is used when a NameNode is returning the result of some FS operation back to a client.
     */
    public static final String OPERATION_RESULT = "RESULT";

    /**
     * Register all the classes that are going to be sent over the network.
     *
     * This must be done on both the client and the server before any network communication occurs.
     * The exact same classes are to be registered in the exact same order.
     * @param kryo The Kryo object obtained from a given Kryo TCP client/server via getKryo().
     */
    public static void registerClassesToBeTransferred(Kryo kryo) {
        // Register the JsonObject class with the Kryo serializer, as this is the object
        // that clients will use to invoke operations on the NN via TCP requests.
        kryo.register(JsonObject.class);
        kryo.register(com.google.gson.internal.LinkedTreeMap.class);
        kryo.register(JsonElement.class);
        kryo.register(JsonPrimitive.class);
        kryo.register(JsonArray.class);
    }
}
