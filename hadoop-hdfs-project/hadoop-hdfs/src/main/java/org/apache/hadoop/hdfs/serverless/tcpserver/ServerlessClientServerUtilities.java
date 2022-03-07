package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.operation.execution.DuplicateRequest;
import org.apache.hadoop.hdfs.serverless.operation.execution.NullResult;

/**
 * Utility functions exposed by both TCP clients and servers.
 */
public class ServerlessClientServerUtilities {
    private static final Log LOG = LogFactory.getLog(ServerlessClientServerUtilities.class);

    /**
     * This operation is used when a NameNode is first connecting to and registering with a client.
     */
    public static final String OPERATION_REGISTER = "REGISTER";

    /**
     * This operation is used when a NameNode is returning the result of some FS operation back to a client.
     */
    public static final String OPERATION_RESULT = "RESULT";

    /**
     * This operation is used when the NameNode just wants to report some information to the client.
     *
     * As of right now, this information will just be logged/use for debugging purposes.
     */
    public static final String OPERATION_INFO = "INFO";

    /**
     * Register all the classes that are going to be sent over the network.
     *
     * This must be done on both the client and the server before any network communication occurs.
     * The exact same classes are to be registered in the exact same order.
     * @param kryo The Kryo object obtained from a given Kryo TCP client/server via getKryo().
     */
    public static synchronized void registerClassesToBeTransferred(Kryo kryo) {
        kryo.register(DuplicateRequest.class);
        kryo.register(NullResult.class);
    }
}
