package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jdk.nashorn.internal.runtime.regexp.joni.ast.StringNode;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;

import java.io.IOException;
import java.net.BindException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessClientServerUtilities.OPERATION_REGISTER;
import static org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessClientServerUtilities.OPERATION_RESULT;

/**
 * Clients of Serverless HopsFS expose a TCP server that serverless NameNodes can connect to.
 *
 * Clients will then issue TCP requests to the serverless NameNodes in order to perform file system operations.
 *
 * This is used on the client side (i.e., NOT on the NameNode side).
 */
public class HopsFSUserServer {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(HopsFSUserServer.class);

    /**
     * The KryoNet server object. This is the actual server itself.
     */
    private final Server server;

    /**
     * The port on which the server is listening.
     */
    private final int tcpPort;

    /**
     * The base name of the NameNode deployments. This is used to query our active connections.
     * We can provide the function number of a name node, which will be appended to this base name in order to
     * query our connections.
     */
    private final String baseFunctionName;

    /**
     * Cache mapping of serverless function names to the connections to that particular serverless function.
     *
     * Each deployment of the serverless name node has a name (e.g., "namenode1", "namenode2", etc.). We map
     * these names to the connection associated with that namenode.
     *
     * This is a concurrent hash map as it will be referenced both by the main thread and background
     * threads when issuing TCP requests to NameNodes.
     */
    private final ConcurrentHashMap<String, NameNodeConnection> activeConnections;

    /**
     * The TCP Server maintains a collection of Futures for clients that are awaiting a response from
     * the NameNode to which they issued a request.
     */
    private final ConcurrentHashMap<String, RequestResponseFuture> activeFutures;

    /**
     * A collection of all the futures that we've completed in one way or another (either they
     * were cancelled or we received a result for them).
     */
    private final ConcurrentHashMap<String, RequestResponseFuture> completedFutures;

    /**
     * Associate with each connection the list of futures that have been submitted and NOT completed.
     *
     * If the connection is lost, then these futures must be re-submitted via HTTP.
     */
    private final ConcurrentHashMap<String, List<RequestResponseFuture>> submittedFutures;

    /**
     * Constructor.
     */
    public HopsFSUserServer(Configuration conf) {
        server = new Server() {
          /**
           * By providing our own connection implementation, we can store per-connection state
           * without a connection ID to perform state look-up.
           */
          @Override
          protected Connection newConnection() {
            LOG.debug("[TCP Server] Creating new NameNodeConnection.");
            return new NameNodeConnection();
          }
        };

        // First, register the JsonObject class with the Kryo serializer.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(server.getKryo());

        addListenersToServer();

        this.tcpPort = conf.getInt(DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT,
                DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT_DEFAULT);
        this.activeConnections = new ConcurrentHashMap<>();
        this.submittedFutures = new ConcurrentHashMap<>();
        this.activeFutures = new ConcurrentHashMap<>();
        this.completedFutures = new ConcurrentHashMap<>();

        String functionEndpoint = conf.get(DFSConfigKeys.SERVERLESS_ENDPOINT,
                DFSConfigKeys.SERVERLESS_ENDPOINT_DEFAULT);

        // The format of the endpoint is something like https://domain:443/api/v1/web/whisk.system/default/<base_name>
        String[] endpointSplit = functionEndpoint.split("/");

        // TODO: Change this hard-coded solution.
        this.baseFunctionName = "/whisk.system/" + endpointSplit[endpointSplit.length - 1];
    }

    /**
     * Stop the TCP server.
     */
    public void stop() {
        LOG.debug("HopsFSUserServer stopping now...");
        this.server.stop();
    }

    /**
     * Start the TCP server.
     */
    public void startServer() throws IOException {
        LOG.debug("Starting HopsFS Client TCP Server now...");

        // Start the TCP server.
        server.start();

        // Bind to the specified TCP port so the server listens on that port.
        LOG.debug("[TCP SERVER] HopsFS Client TCP Server binding to port " + tcpPort + " now...");

        try {
            server.bind(tcpPort);
        }
        catch (BindException ex) {
            throw new IOException("[TCP SERVER] TCP Server encountered BindException while attempting to bind to port "
                    + tcpPort + ". Do you already have a serving running on that port?");
        }
    }

    /**
     * Add the various Listeners/network handlers to the TCP server.
     */
    private void addListenersToServer() {
        // We need to add some listeners to the server. This is how we add functionality.
        server.addListener(new Listener() {
            /**
             * Listener handles connection establishment with remote NameNodes.
             */
            public void connected(Connection conn) {
                LOG.debug("[TCP Server] Connection established with remote NameNode at "
                        + conn.getRemoteAddressTCP());
            }

            /**
             * This listener handles receiving TCP messages from the name nodes.
             * @param conn The connection to the name node.
             * @param object The object that was sent by the name node to the client (us).
             */
            public void received(Connection conn, Object object) {
                NameNodeConnection connection = (NameNodeConnection)conn;

                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof String) {
                    JsonObject body = new JsonParser().parse((String)object).getAsJsonObject();
                            //JsonParser.parseString((String)object).getAsJsonObject();

                    LOG.debug("[TCP Server] Received message from NameNode at " + connection.toString() + " at " +
                            connection.getRemoteAddressTCP() + ".");

                    LOG.debug("===== Message Contents =====");
                    for (String key : body.keySet())
                        LOG.debug("     " + key + ": " + body.getAsJsonPrimitive(key).toString());
                    LOG.debug("============================");

                    String functionName = body.getAsJsonPrimitive(ServerlessNameNodeKeys.FUNCTION_NAME).getAsString();
                    String operation = body.getAsJsonPrimitive("op").getAsString();

                    String requestId = null;

                    // There won't be a requestId during registration attempts, just when results are being returned.
                    if (body.has("requestId"))
                        requestId = body.getAsJsonPrimitive("requestId").getAsString();

                    LOG.debug("[TCP SERVER] FunctionName: " + functionName + ", RequestID: " + requestId
                            + ", Operation: " + operation);

                    // There are currently two different operations that a NameNode may perform.
                    // The first is registration. This operation results in the connection to the NameNode
                    // being cached locally by the client. The second operation is that of returning a result
                    // of a file system operation back to the user.
                    switch (operation) {
                        // The NameNode is registering with the client (i.e., connecting for the first time,
                        // or at least they are connecting after having previously lost connection).
                        case OPERATION_REGISTER:
                            LOG.debug("[TCP SERVER] Received registration operation from NameNode " + functionName);
                            registerNameNode(connection, functionName);
                            break;
                        // The NameNode is returning a result (of a file system operation) to the client.
                        case OPERATION_RESULT:
                            LOG.debug("[TCP SERVER] Received result from NameNode " + functionName);

                            // If there is no request ID, then we have no idea which operation this result is
                            // associated with, and thus we cannot do anything with it.
                            if (requestId == null) {
                                LOG.warn("[TCP Server] TCP Server received response containing result of FS " +
                                        "operation, but response did not contain a request ID.");
                                break;
                            }

                            RequestResponseFuture future = activeFutures.getOrDefault(requestId, null);

                            // If there is no future associated with this operation, then we have no means to return
                            // the result back to the client who issued the file system operation.
                            if (future == null) {
                                LOG.warn("[TCP SERVER] TCP Server received response for request " + requestId +
                                        ", but there is no associated future registered with the server.");
                                break;
                            }

                            future.postResult(body);

                            // Update state pertaining to futures.
                            activeFutures.remove(requestId);
                            completedFutures.put(requestId, future);
                            break;
                        default:
                            LOG.warn("[TCP SERVER] Unknown operation received from NameNode " + functionName + ": "
                                    + operation);
                    }
                }
                else if (object instanceof FrameworkMessage.KeepAlive) {
                    // The server periodically sends KeepAlive objects to prevent the client from disconnecting
                    // due to timeouts. Just ignore these (i.e., do nothing).
                }
                else {
                    LOG.warn("[TCP SERVER] Received object of unexpected type from remote client " + connection +
                            " at " + connection.getRemoteAddressTCP() + ". Object type: " +
                            object.getClass().getSimpleName() + ".");
                }
            }

            /**
             * Handle the disconnection of a NameNode from the client.
             *
             * Remove the associated connection from the active connections cache.
             */
            public void disconnected(Connection conn) {
                NameNodeConnection connection = (NameNodeConnection)conn;

                if (connection.name != null) {
                    LOG.debug("[TCP SERVER] Connection to " + connection.name + " lost.");
                    activeConnections.remove(connection.name);

                    List<RequestResponseFuture> incompleteFutures = submittedFutures.get(connection.name);

                    LOG.warn("There were " + incompleteFutures.size()
                            + " incomplete future(s) associated with now-terminated connection " +
                            connection.name);

                    // Cancel each of the futures.
                    for (RequestResponseFuture future : incompleteFutures) {
                        LOG.debug("    Cancelling future " + future.getRequestId() + " for operation " +
                                future.getOperationName());
                        try {
                            future.cancel(ServerlessNameNodeKeys.REASON_CONNECTION_LOST, true);
                        } catch (InterruptedException ex) {
                            LOG.error("Error encountered while cancelling future " + future.getRequestId()
                                    + " for operation " + future.getOperationName() + ":", ex);
                        }
                    }
                } else {
                    LOG.warn("[TCP SERVER] Lost connection to unregistered NameNode.");
                }
            }
        });
    }

    /**
     * Register the remote serverless NameNode locally. This involves assigning a name to the connection
     * object as well as caching the active connection locally.
     * @param connection The connection to the serverless name node.
     * @param functionName The (unique) name of the serverless name node.
     */
    private void registerNameNode(NameNodeConnection connection, String functionName) {
        connection.name = functionName;

        cacheConnection(connection, functionName);
    }

    /**
     * Cache an active connection with a serverless name node locally. This will fail (and return false) if there
     * is already a connection associated with the given functionName cached.
     *
     * @param connection The connection with the name node.
     * @param functionName The name of the function to which we are connected.
     * @return true if the function was cached successfully, false if we already have this connection cached.
     */
    private void cacheConnection(NameNodeConnection connection, String functionName) {
        if (activeConnections.containsKey(functionName)) {
            throw new IllegalStateException("[TCP SERVER] Connection with NameNode " + functionName
                    + " already cached locally. " + "Currently cached connection: "
                    + activeConnections.get(functionName).toString() + ", new connection: " + connection.toString());
        }

        LOG.debug("Caching active connection with serverless function \"" + functionName + "\" now...");
        activeConnections.put(functionName, connection);
    }

    /**
     * Get the TCP connection associated with the NameNode deployment identified by the given function number.
     *
     * Returns null if no such connection exists.
     * @param functionNumber The NameNode deployment for which the connection is desired.
     * @return TCP connection to the desired NameNode if it exists, otherwise null.
     */
    private Connection getConnection(int functionNumber) {
        String serverlessFunctionName = baseFunctionName + functionNumber;

        return activeConnections.getOrDefault(serverlessFunctionName, null);
    }

    /**
     * Remove the connection to the NameNode identified by the given functionNumber from the connection cache.
     *
     * If the connection is still active, it will only be removed if the `deleteIfActive` flag is set to true.
     * In this scenario, the connection will first be closed before it is removed from the connection cache.
     * @param functionNumber The deployment number of the name node in question.
     * @param deleteIfActive Flag indicating whether the connection should still be closed if it is currently
     *                       active.
     * @param errorIfActive Throw an error if the function is found to be active. This is useful if we believe the
     *                      connection is already closed and that is why we are removing it.
     * @return True if a connection was removed, otherwise false.
     */
    private boolean deleteConnection(int functionNumber, boolean deleteIfActive, boolean errorIfActive) {
        String functionName = baseFunctionName + functionNumber;

        Connection conn = activeConnections.getOrDefault(functionName, null);

        if (conn != null) {
            if (conn.isConnected()) {

                if (errorIfActive) {
                    throw new IllegalStateException("[TCP SERVER] Connection to " + functionName
                            + " was found to be active.");
                }

                if (deleteIfActive) {
                    conn.close();
                    activeConnections.remove(functionName);
                    LOG.debug("[TCP SERVER] Closed and removed connection to " + functionName);
                    return true;
                } else {
                    LOG.debug("[TCP SERVER] Cannot remove connection to " + functionName
                            + " because it is still active " + "(and the override flag was not set to true).");
                    return false;
                }
            } else {
                activeConnections.remove(functionName);
                LOG.debug("[TCP SERVER] Removed already-closed connection to " + functionName);
                return true;
            }
        } else {
            LOG.warn("[TCP SERVER] Cannot remove connection to " + functionName + ". No such connection exists!");
            return false;
        }
    }

    /**
     * Checks if there is an active connection established to the NameNode with the given function number.
     *
     * @param functionNumber The function number of the desired NameNode.
     * @return True if a connection currently exists, otherwise false.
     */
    public boolean connectionExists(int functionNumber) {
        Connection tcpConnection = getConnection(functionNumber);

        if(tcpConnection != null) {
            if (tcpConnection.isConnected())
                return true;

            // If the connection is NOT active, then we need to remove it from our cache of connections.
            deleteConnection(functionNumber, false, true);
        }

        return false;
    }

    public void printDebugInformation() {
        LOG.debug("========== TCP Server Debug Information ==========");
        LOG.debug("CONNECTIONS:");
        LOG.debug("     Number of active connections: " + activeConnections.size());
        ConcurrentHashMap.KeySetView<String, NameNodeConnection> keySetView = activeConnections.keySet();
        LOG.debug("     Connected to:");
        keySetView.forEach(funcName -> LOG.debug("         " + funcName));
        LOG.debug("FUTURES:");
        LOG.debug("     Number of active futures: " + activeFutures.size());
        LOG.debug("     Number of completed futures: " + completedFutures.size());
        LOG.debug("==================================================");
    }

    /**
     * Register a RequestResponseFuture with the server. The server will post the NameNode's response for the
     * associated request to the Future.
     *
     * Checks for duplicate futures before registering it.
     * @return True if the Future was registered successfully.
     */
    public boolean registerRequestResponseFuture(RequestResponseFuture requestResponseFuture) {
        if (activeFutures.containsKey(requestResponseFuture.getRequestId()) ||
            completedFutures.containsKey(requestResponseFuture.getRequestId())) {
            return false;
        }

        activeFutures.put(requestResponseFuture.getRequestId(), requestResponseFuture);
        return true;
    }

    /**
     * Issue a TCP request to the given NameNode. Ths function will check to ensure that the connection exists
     * first before issuing the connection.
     * @param functionNumber The NameNode to issue a request to.
     * @param bypassCheck Do not check if the connection exists.
     * @param payload The payload to send to the NameNode in the TCP request.
     * @return A Future representing the eventual response from the NameNode.
     */
    public RequestResponseFuture issueTcpRequest(int functionNumber, boolean bypassCheck, JsonObject payload) {
        if (!bypassCheck && !connectionExists(functionNumber)) {
            LOG.warn("[TCP SERVER] Was about to issue TCP request to NameNode deployment " + functionNumber +
                    ", but connection no longer exists...");
            return null;
        }

        // Send the TCP request to the NameNode.
        Connection tcpConnection = getConnection(functionNumber);
        int bytesSent = tcpConnection.sendTCP(payload.toString());

        LOG.debug("Sent " + bytesSent + " bytes to NameNode" + functionNumber + ".");

        // Create and register a future to keep track of this request and provide a means for the client to obtain
        // a response from the NameNode, should the client deliver one to us.
        String requestId = payload.get("requestId").getAsString();
        String operation = payload.get("op").getAsString();
        RequestResponseFuture requestResponseFuture = new RequestResponseFuture(requestId, operation);
        registerRequestResponseFuture(requestResponseFuture);

        return requestResponseFuture;
    }

    /**
     * Issue a TCP request to the given NameNode. Ths function will check to ensure that the connection exists
     * first before issuing the connection.
     *
     * This function then waits for a response from the NameNode to be returned.
     *
     * This should NOT be called from the main thread.
     * @param functionNumber The NameNode to issue a request to.
     * @param bypassCheck Do not check if the connection exists.
     * @param payload The payload to send to the NameNode in the TCP request.
     * @return The response from the NameNode, or null if the request failed for some reason.
     */
    public JsonObject issueTcpRequestAndWait(int functionNumber, boolean bypassCheck, JsonObject payload)
            throws ExecutionException, InterruptedException {
        RequestResponseFuture requestResponseFuture = issueTcpRequest(functionNumber, bypassCheck, payload);

        LOG.debug("[TCP SERVER] Waiting for result from future for request " + requestResponseFuture.getRequestId()
                + "," + "associated serverless function number = " + functionNumber);
        return requestResponseFuture.get();
    }

    /**
     * Wrapper around Kryo connection objects in order to track per-connection state without needing to use
     * connection IDs to perform state look-up.
     */
    static class NameNodeConnection extends Connection {
        public String name;

        @Override
        public String toString() {
            return this.name != null ? this.name : super.toString();
        }
    }
}