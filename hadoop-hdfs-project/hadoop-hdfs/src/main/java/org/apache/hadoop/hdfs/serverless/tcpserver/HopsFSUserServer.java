package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.google.gson.JsonObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessClientServerUtilities.OPERATION_REGISTER;
import static org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessClientServerUtilities.OPERATION_RESULT;

/**
 * Clients of Serverless HopsFS expose a TCP server that serverless NameNodes can connect to.
 *
 * Clients will then issue TCP requests to the serverless NameNodes in order to perform file system operations.
 */
public class HopsFSUserServer {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(HopsFSUserServer.class);

    /**
     *
     */
    private final Server server;

    /**
     *
     */
    private final int tcpPort;

    /**
     *
     */
    private final HashMap<String, NameNodeConnection> activeConnections;

    /**
     * Constructor.
     *
     * @param tcpPort The TCP port on which the server will listen.
     */
    public HopsFSUserServer(Configuration conf, int tcpPort) {
        server = new Server() {
          protected Connection newConnection() {
              /**
               * By providing our own connection implementation, we can store per-connection state
               * without a connection ID to perform state look-up.
               */
              return new NameNodeConnection();
          }
        };

        this.tcpPort = conf.getInt(DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT,
                DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT_DEFAULT);
        this.activeConnections = new HashMap<>();
    }

    /**
     * Start the TCP server.
     */
    public void startServer() throws IOException {
        // First, register the JsonObject class with the Kryo serializer.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(server.getKryo());

        server.start();
        server.bind(tcpPort);

        server.addListener(new Listener() {
            public void received(Connection connection, Object object) {
                LOG.debug("Received message from connection " + connection.toString());

                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof JsonObject) {
                    JsonObject body = (JsonObject)object;

                    LOG.debug("Message contents: " + body);

                    String functionName = body.getAsJsonPrimitive("functionName").getAsString();
                    String operation = body.getAsJsonPrimitive("op").getAsString();

                    // There are currently two different operations that a NameNode may perform.
                    // The first is registration. This operation results in the connection to the NameNode
                    // being cached locally by the client. The second operation is that of returning a result
                    // of a file system operation back to the user.
                    switch (operation) {
                        case OPERATION_REGISTER:
                            LOG.debug("Received registration operation from NameNode " + functionName);
                            break;
                        case OPERATION_RESULT:
                            LOG.debug("Received result from NameNode " + functionName);
                            break;
                        default:
                            LOG.warn("Unknown operation received from NameNode " + functionName + ": " + operation);
                    }

                    cacheConnection(connection, functionName);
                }
                else {
                    throw new IllegalArgumentException("Received object of unexpected type from remote client "
                            + connection + ". Object type: " + object.getClass().getSimpleName() + ".");
                }
            }

            /**
             * Handle the disconnection of a NameNode from the client.
             *
             * Remove the associated connection from the active connections cache.
             */
            public void disconnected(Connection connection) {
                NameNodeConnection conn = (NameNodeConnection)connection;

                if (conn.name != null) {
                    LOG.debug("Connection to " + conn.name + " lost.");

                    activeConnections.remove(conn.name);
                } else {
                    LOG.warn("Lost connection to unknown NameNode...");
                }
            }
        });
    }

    /**
     * Cache an active connection with a serverless name node locally.
     * @param connection The connection with the name node.
     * @param functionName The name of the function to which we are connected.
     * @return true if the function was cached successfully, false if we already have this connection cached.
     */
    private boolean cacheConnection(Connection connection, String functionName) {
        NameNodeConnection conn = (NameNodeConnection)connection;

        if (!activeConnections.containsKey(functionName)) {
            LOG.debug("Caching active connection with serverless function \"" +
                    functionName + "\" now...");
            activeConnections.put(functionName, conn);

            conn.name = functionName;

            return true;
        }

        return false;
    }

    /**
     * Wrapper around Kryo connection objects in order to track per-connection state without needing to use
     * connection IDs to perform state look-up.
     */
    static class NameNodeConnection extends Connection {
        public String name;
    }
}