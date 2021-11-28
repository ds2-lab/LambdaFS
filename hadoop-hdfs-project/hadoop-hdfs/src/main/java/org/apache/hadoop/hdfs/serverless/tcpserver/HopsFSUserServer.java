package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessNameNodeClient;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
    private int tcpPort;

    /**
     * Cache mapping of serverless function names to the connections to that particular serverless function.
     *
     * We map functions based on their unique IDs, which are longs, but we convert them to strings as connection names
     * are strings.
     *
     * This is a concurrent hash map as it will be referenced both by the main thread and background
     * threads when issuing TCP requests to NameNodes.
     */
    private final ConcurrentHashMap<Long, NameNodeConnection> allActiveConnections;

    /**
     * Caches the active connections in lists per deployment.
     */
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, NameNodeConnection>> activeConnectionsPerDeployment;

    /**
     * The TCP Server maintains a collection of Futures for clients that are awaiting a response from
     * the NameNode to which they issued a request.
     */
    private final ConcurrentHashMap<String, RequestResponseFuture> activeFutures;

    /**
     * We also map the unique IDs of NameNodes to their deployments. This is used for debugging/logging and for
     * obtaining a connection to a NameNode from a specific deployment when invoking NNs.
     */
    private final ConcurrentHashMap<Long, Integer> nameNodeIdToDeploymentMapping;

    /**
     * A collection of all the futures that we've completed in one way or another (either they
     * were cancelled or we received a result for them).
     */
    private final ConcurrentHashMap<String, RequestResponseFuture> completedFutures;

    /**
     * Associate with each connection the list of futures that have been submitted and NOT completed.
     * The keys are NameNode IDs.
     *
     * If the connection is lost, then these futures must be re-submitted via HTTP.
     */
    private final ConcurrentHashMap<Long, List<RequestResponseFuture>> submittedFutures;

    /**
     * Mapping of task/request ID to the NameNode to which the task/request was submitted.
     */
    private final ConcurrentHashMap<String, NameNodeConnection> futureToNameNodeMapping;

    /**
     * Indicates whether the TCP server should ultimately be started and enabled.
     */
    private final boolean enabled;

    /**
     * Number of unique deployments.
     */
    private final int totalNumberOfDeployments;

    /**
     * We need a reference to this so that we can tell it what TCP port we ultimated bound to.
     */
    private final ServerlessNameNodeClient client;

    /**
     * Constructor.
     */
    public HopsFSUserServer(Configuration conf, ServerlessNameNodeClient client) {
        server = new Server(64000, 64000) {
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

        //Log.set(Log.LEVEL_TRACE);

        enabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED,
                DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);
        totalNumberOfDeployments = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS,
                DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

        LOG.info("TCP server " + (enabled ? "ENABLED." : "DISABLED."));

        // First, register the JsonObject class with the Kryo serializer.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(server.getKryo());

        addListenersToServer();

        this.tcpPort = conf.getInt(DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT,
                DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT_DEFAULT);
        this.allActiveConnections = new ConcurrentHashMap<>();
        this.submittedFutures = new ConcurrentHashMap<>();
        this.activeFutures = new ConcurrentHashMap<>();
        this.completedFutures = new ConcurrentHashMap<>();
        this.futureToNameNodeMapping = new ConcurrentHashMap<>();
        this.nameNodeIdToDeploymentMapping = new ConcurrentHashMap<>();
        this.activeConnectionsPerDeployment = new ConcurrentHashMap<>();
        this.client = client;

        // Populate the active connections mapping with default, empty hash maps for each deployment.
        for (int deployNum = 0; deployNum < totalNumberOfDeployments; deployNum++) {
            activeConnectionsPerDeployment.put(deployNum, new ConcurrentHashMap<>());
        }

        String functionEndpoint = conf.get(DFSConfigKeys.SERVERLESS_ENDPOINT,
                DFSConfigKeys.SERVERLESS_ENDPOINT_DEFAULT);

        // The format of the endpoint is something like https://domain:443/api/v1/web/whisk.system/default/<base_name>
        String[] endpointSplit = functionEndpoint.split("/");
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
        if (!enabled) {
            LOG.warn("TCP Server is NOT enabled. Server will NOT be started.");
            return;
        }
        LOG.debug("Starting HopsFS Client TCP Server now...");

        // Start the TCP server.
        server.start();

        // Bind to the specified TCP port so the server listens on that port.
        LOG.debug("[TCP SERVER] HopsFS Client TCP Server binding to port " + tcpPort + " now...");

        int maxPort = tcpPort + 1000;
        int currentPort = tcpPort;
        boolean success = false;
        while (currentPort < maxPort && !success) {
            try {
                LOG.debug("[TCP Server] Trying to bind to port " + currentPort + ".");
                server.bind(currentPort);

                if (tcpPort != currentPort) {
                    LOG.warn("[TCP Server] Configuration specified port " + tcpPort +
                            ", but we were unable to bind to that port. Instead, we are bound to port " + currentPort +
                            ".");
                    this.tcpPort = currentPort;
                }

                success = true;
            } catch (BindException ex) {
                LOG.error("[TCP Server] Failed to bind to port " + currentPort +
                        ". Do you already have a server running on that port?");
                currentPort++;
            }
        }

        if (!success)
            throw new IOException("Failed to start TCP server. Could not successfully bind to any ports.");

        client.setTcpServerPort(tcpPort);
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

                    try {
                        LOG.debug("===== Message Contents =====");
                        for (String key : body.keySet()) {
                            try {
                                // Don't print results, statistics packages, or transaction events as they're too long.
                                if (key.equals(ServerlessNameNodeKeys.RESULT))
                                    LOG.debug("     " + key + ": <RESULT>");
                                else if (key.equals(ServerlessNameNodeKeys.STATISTICS_PACKAGE))
                                    LOG.debug("     " + key + ": <STATISTICS PACKAGE>");
                                else if (key.equals(ServerlessNameNodeKeys.TRANSACTION_EVENTS))
                                    LOG.debug("     " + key + ": " + "<TRANSACTION EVENTS>");
                                else
                                    LOG.debug("     " + key + ": " + body.getAsJsonPrimitive(key).toString());
                            } catch (ClassCastException ex) {
                                LOG.debug("     " + key + ": " + body.getAsJsonArray(key).toString());
                            }
                        }
                        LOG.debug("============================");
                    } catch (Exception ex) {
                        LOG.error("Unexpected error encountered while iterating over keys of message:", ex);
                        LOG.debug("Printing message in its entirety.");
                        LOG.debug(body.toString());
                    }

                    int deploymentNumber = body.getAsJsonPrimitive(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER).getAsInt();
                    long nameNodeId = body.getAsJsonPrimitive(ServerlessNameNodeKeys.NAME_NODE_ID).getAsLong();
                    String operation = body.getAsJsonPrimitive("op").getAsString();

                    String requestId = null;

                    // There won't be a requestId during registration attempts, just when results are being returned.
                    if (body.has("requestId"))
                        requestId = body.getAsJsonPrimitive("requestId").getAsString();

                    LOG.debug("[TCP SERVER] NN ID: " + nameNodeId + ", Deployment #: " + deploymentNumber +
                            ", RequestID: " + requestId + ", Operation: " + operation);

                    // There are currently two different operations that a NameNode may perform.
                    // The first is registration. This operation results in the connection to the NameNode
                    // being cached locally by the client. The second operation is that of returning a result
                    // of a file system operation back to the user.
                    switch (operation) {
                        // The NameNode is registering with the client (i.e., connecting for the first time,
                        // or at least they are connecting after having previously lost connection).
                        case OPERATION_REGISTER:
                            LOG.debug("[TCP SERVER] Received registration operation from NameNode " +
                                    nameNodeId + ", Deployment #" + deploymentNumber);
                            registerNameNode(connection, deploymentNumber, nameNodeId);
                            break;
                        // The NameNode is returning a result (of a file system operation) to the client.
                        case OPERATION_RESULT:
                            LOG.debug("[TCP SERVER] Received result from NameNode " + nameNodeId +
                                    ", Deployment #" + deploymentNumber);

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

                            List<RequestResponseFuture> incompleteFutures = submittedFutures.get(connection.name);
                            incompleteFutures.remove(future);

                            break;
                        default:
                            LOG.warn("[TCP SERVER] Unknown operation received from NameNode " + nameNodeId +
                                    ", Deployment #" + deploymentNumber + ": '" + operation + "'");
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

                if (connection.name != -1) {
                    LOG.debug("[TCP SERVER] Connection to NN" + connection.name + " lost.");
                    allActiveConnections.remove(connection.name);

                    int mappedDeploymentNumber = nameNodeIdToDeploymentMapping.get(connection.name);
                    ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                            activeConnectionsPerDeployment.get(mappedDeploymentNumber);
                    deploymentConnections.remove(connection.name);

                    List<RequestResponseFuture> incompleteFutures = submittedFutures.get(connection.name);

                    if (incompleteFutures == null) {
                        LOG.debug("There were no futures associated with now-closed connection " + connection.name);
                        return;
                    }

                    LOG.warn("There were " + incompleteFutures.size()
                            + " incomplete future(s) associated with now-terminated connection " + connection.name);

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
     * @param nameNodeId The unique ID of the NameNode.
     * @param deploymentNumber The deployment in which the NameNode is running.
     */
    private void registerNameNode(NameNodeConnection connection, int deploymentNumber, long nameNodeId) {
        connection.name = nameNodeId;

        cacheConnection(connection, deploymentNumber, nameNodeId);
    }

    /**
     * Cache an active connection with a serverless name node locally. This will fail (and return false) if there
     * is already a connection associated with the given functionName cached.
     *
     * @param connection The connection with the name node.
     * @param nameNodeId The unique ID of the NameNode.
     * @param deploymentNumber The deployment in which the NameNode is running.
     */
    private void cacheConnection(NameNodeConnection connection, int deploymentNumber, long nameNodeId) {
        if (allActiveConnections.containsKey(nameNodeId)) {
            NameNodeConnection oldConnection = allActiveConnections.get(nameNodeId);
            // Sanity check.
            int existingDeploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);

            if (existingDeploymentNumber != deploymentNumber)
                throw new IllegalStateException("Received connection from NN " + nameNodeId +
                        ". NN currently reports deployment # as " + deploymentNumber +
                        ", but we prev. cached its deployment # as " + existingDeploymentNumber);

            if (oldConnection.isConnected()) {
                LOG.warn("[TCP Server] Already have an ACTIVE conn to NameNode " + nameNodeId +
                        " (deployment #" + deploymentNumber + ".");
                LOG.warn("[TCP Server] Replacing old, ACTIVE conn to NameNode " + nameNodeId +
                        " (deployment #" + deploymentNumber + ") with a new one...");

                oldConnection.close();
            } else {
                LOG.error("[TCP Server] Already have a conn to NameNode " + nameNodeId + " (deployment #" +
                        deploymentNumber + "), but it is apparently no longer connected...");
                LOG.warn("[TCP Server] Replacing old, now-disconnected conn to NameNode " + nameNodeId +
                        " (deployment #" + deploymentNumber + ") with the new one...");
            }
        } else {
            // We don't want to print this debug message along with the ones from the if-statement above, so
            // we put it in the else block. It isn't contradictory or anything, but it'd be redundant.
            LOG.debug("Caching connection to NN " + nameNodeId + " (deployment #" + deploymentNumber + ") now.");
        }

        allActiveConnections.put(nameNodeId, connection);
        activeConnectionsPerDeployment.get(deploymentNumber).put(nameNodeId, connection);
        nameNodeIdToDeploymentMapping.put(nameNodeId, deploymentNumber);
    }

    /**
     * Get the TCP connection associated with the NameNode deployment identified by the given function number.
     *
     * Returns null if no such connection exists.
     * @param nameNodeId The unique ID of the NameNode for which the connection is desired.
     * @return TCP connection to the desired NameNode if it exists, otherwise null.
     */
    private NameNodeConnection getConnection(long nameNodeId) {
        return allActiveConnections.getOrDefault(nameNodeId, null);
    }

    /**
     * Get a TCP connection for a NameNode from the specified deployment. Returns a random, active connection if one
     * exists. Otherwise, returns null.
     */
    private NameNodeConnection getConnection(int deploymentNumber) {
        ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                activeConnectionsPerDeployment.get(deploymentNumber);

        // Return a random NameNode connection.
        Random rng = new Random();
        NameNodeConnection[] values = deploymentConnections.values().toArray(new NameNodeConnection[0]);

        if (values.length == 0)
            return null;

        return values[rng.nextInt(values.length)];
    }

    /**
     * Remove the connection to the NameNode identified by the given functionNumber from the connection cache.
     *
     * If the connection is still active, it will only be removed if the `deleteIfActive` flag is set to true.
     * In this scenario, the connection will first be closed before it is removed from the connection cache.
     * @param nameNodeId The unique ID of the NN for which the connection should be deleted.
     * @param deleteIfActive Flag indicating whether the connection should still be closed if it is currently
     *                       active.
     * @param errorIfActive Throw an error if the function is found to be active. This is useful if we believe the
     *                      connection is already closed and that is why we are removing it.
     * @return True if a connection was removed, otherwise false.
     */
    private boolean deleteConnection(long nameNodeId, boolean deleteIfActive, boolean errorIfActive) {
        NameNodeConnection conn = allActiveConnections.getOrDefault(nameNodeId, null);

        if (conn != null) {
            if (conn.isConnected()) {

                if (errorIfActive) {
                    throw new IllegalStateException("[TCP SERVER] Connection to NN " + nameNodeId
                            + " was found to be active.");
                }

                if (deleteIfActive) {
                    conn.close();
                    allActiveConnections.remove(nameNodeId);

                    // Remove from the mapping for the NN's specific deployment.
                    int deploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);
                    ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                            activeConnectionsPerDeployment.get(deploymentNumber);
                    deploymentConnections.remove(nameNodeId);


                    // Remove the list of futures associated with this connection.
                    // TODO: Should we resubmit these via HTTP? Or just drop them, effectively?
                    //       Currently, we're just dropping them.
                    List<RequestResponseFuture> incompleteFutures = submittedFutures.get(conn.name);
                    if (incompleteFutures.size() > 0) {
                        LOG.warn("Connection to NameNode " + nameNodeId + " has " + incompleteFutures.size() +
                                " incomplete futures associated with it, yet we're deleting the connection...");
                    }
                    submittedFutures.remove(conn.name);

                    LOG.debug("[TCP SERVER] Closed and removed connection to NN " + nameNodeId);
                    return true;
                } else {
                    LOG.debug("[TCP SERVER] Cannot remove connection to NN " + nameNodeId
                            + " because it is still active " + "(and the override flag was not set to true).");
                    return false;
                }
            } else {
                allActiveConnections.remove(nameNodeId);

                // Remove from the mapping for the NN's specific deployment.
                int deploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);
                ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                        activeConnectionsPerDeployment.get(deploymentNumber);
                deploymentConnections.remove(nameNodeId);

                LOG.debug("[TCP SERVER] Removed already-closed connection to NN " + nameNodeId);
                return true;
            }
        } else {
            LOG.warn("[TCP SERVER] Cannot remove connection to NN " + nameNodeId + ". No such connection exists!");
            return false;
        }
    }

    /**
     * Check if there exists at least one connection to a NameNode in the specified deployment.
     *
     * @param deploymentNumber The deployment to which we're asking if at least one connection exists.
     * @return True if a connection currently exists, otherwise false.
     */
    public boolean connectionExists(int deploymentNumber) {
        if (deploymentNumber == -1)
            return false;

        ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                activeConnectionsPerDeployment.get(deploymentNumber);

        if (deploymentConnections == null)
            throw new IllegalStateException("Deployment connections is null for deployment " + deploymentNumber);

        return deploymentConnections.size() > 0;
    }

    /**
     * Checks if there is an active connection established to the NameNode with the given ID.
     *
     * @param nameNodeId The ID of the NN for which we're querying the existence of a connection.
     * @return True if a connection currently exists, otherwise false.
     */
    public boolean connectionExists(long nameNodeId) {
        Connection tcpConnection = getConnection(nameNodeId);

        if(tcpConnection != null) {
            if (tcpConnection.isConnected())
                return true;

            // If the connection is NOT active, then we need to remove it from our cache of connections.
            deleteConnection(nameNodeId, false, true);
            LOG.warn("Found that connection to NN " + nameNodeId + " is NOT connected while checking" +
                    " if it exists. Removing it from the connection mapping...");
        }

        return false;
    }

    public void printDebugInformation() {
        LOG.debug("========== TCP Server Debug Information ==========");
        LOG.debug("CONNECTIONS:");
        LOG.debug("     Number of active connections: " + allActiveConnections.size());
        LOG.debug("     Connected to:");
        for (Map.Entry<Integer, ConcurrentHashMap<Long, NameNodeConnection>> entry : activeConnectionsPerDeployment.entrySet()) {
            int deploymentNumber = entry.getKey();
            ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections = entry.getValue();
            LOG.debug("     Deployment #" + deploymentNumber + ": ");

            if (deploymentConnections.size() == 0) {
                LOG.debug("               No connections established");
                continue;
            }

            ConcurrentHashMap.KeySetView<Long, NameNodeConnection> keySetView = deploymentConnections.keySet();
            keySetView.forEach(funcName -> LOG.debug("               " + funcName));
        }
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
     */
    public void registerRequestResponseFuture(RequestResponseFuture requestResponseFuture) {
        if (activeFutures.containsKey(requestResponseFuture.getRequestId()) ||
            completedFutures.containsKey(requestResponseFuture.getRequestId())) {
            return;
        }

        LOG.debug("[TCP Server] Registering future for request " + requestResponseFuture.getRequestId() + ".");
        activeFutures.put(requestResponseFuture.getRequestId(), requestResponseFuture);
    }

    /**
     * Inform the TCP server that a future has been resolved via HTTP, and as such it should be moved
     * out of the active futures mapping.
     * @param requestId The ID of the request/task that was resolved via TCP.
     */
    public boolean deactivateFuture(String requestId) {
        RequestResponseFuture future = activeFutures.remove(requestId);

        if (future != null) {
            completedFutures.put(requestId, future);

            if (futureToNameNodeMapping.containsKey(requestId)) {
                NameNodeConnection conn = futureToNameNodeMapping.get(requestId);

                // Remove this future from the submitted futures list associated with the connection,
                // if it exists.
                if (conn != null && submittedFutures.containsKey(conn.name)) {
                    List<RequestResponseFuture> futures = submittedFutures.get(conn.name);
                    futures.remove(future);
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Issue a TCP request to the given NameNode. Ths function will check to ensure that the connection exists
     * first before issuing the connection.
     * @param deploymentNumber The NameNode to issue a request to.
     * @param bypassCheck Do not check if the connection exists.
     * @param payload The payload to send to the NameNode in the TCP request.
     * @return A Future representing the eventual response from the NameNode.
     */
    public RequestResponseFuture issueTcpRequest(int deploymentNumber, boolean bypassCheck, JsonObject payload) {
        if (!bypassCheck && !connectionExists(deploymentNumber)) {
            LOG.warn("[TCP SERVER] Was about to issue TCP request to NameNode deployment " + deploymentNumber +
                    ", but connection no longer exists...");
            return null;
        }

        // Create and register a future to keep track of this request and provide a means for the client to obtain
        // a response from the NameNode, should the client deliver one to us.
        String requestId = payload.get("requestId").getAsString();
        String operation = payload.get("op").getAsString();
        RequestResponseFuture requestResponseFuture = new RequestResponseFuture(requestId, operation);
        registerRequestResponseFuture(requestResponseFuture);

        // Send the TCP request to the NameNode.
        NameNodeConnection tcpConnection = getConnection(deploymentNumber);

        if (tcpConnection == null) {
            LOG.warn("[TCP SERVER] Was about to issue TCP request to NameNode deployment " + deploymentNumber +
                    ", but connection no longer exists...");
            return null;
        }

        int bytesSent = tcpConnection.sendTCP(payload.toString());

        // If 'bytesSent' is zero, then an error must have occurred.
        // TODO: Handle this scenario somehow (resubmit the TCP request, notify the client, etc.)
        if (bytesSent == 0)
            LOG.error("Transmission of TCP request " + requestId + " sent 0 bytes.");
        else
            LOG.debug("Sent " + bytesSent + " bytes to NameNode" + deploymentNumber + ".");

        // Make note of this future as being incomplete.
        List<RequestResponseFuture> incompleteFutures = submittedFutures.computeIfAbsent(
                tcpConnection.name, k -> new ArrayList<>());

        incompleteFutures.add(requestResponseFuture);
        futureToNameNodeMapping.put(requestId, tcpConnection);

        return requestResponseFuture;
    }

    /**
     * Issue a TCP request to the given NameNode. Ths function will check to ensure that the connection exists
     * first before issuing the connection.
     *
     * This function then waits for a response from the NameNode to be returned.
     *
     * This should NOT be called from the main thread.
     * @param deploymentNumber The NameNode to issue a request to.
     * @param bypassCheck Do not check if the connection exists.
     * @param payload The payload to send to the NameNode in the TCP request.
     * @return The response from the NameNode, or null if the request failed for some reason.
     */
    public JsonObject issueTcpRequestAndWait(int deploymentNumber, boolean bypassCheck, JsonObject payload)
            throws ExecutionException, InterruptedException {
        RequestResponseFuture requestResponseFuture = issueTcpRequest(deploymentNumber, bypassCheck, payload);

        if (requestResponseFuture == null) {
            LOG.error("Issuing TCP request returned null instead of future. Must have been no connections.");
            return null;
        }

        LOG.debug("[TCP SERVER] Waiting for result from future for request " + requestResponseFuture.getRequestId()
                + ", associated serverless function NameNode " + deploymentNumber);
        return requestResponseFuture.get();
    }

    /**
     * Wrapper around Kryo connection objects in order to track per-connection state without needing to use
     * connection IDs to perform state look-up.
     */
    static class NameNodeConnection extends Connection {
        /**
         * Name of the connection. It's just the unique ID of the NameNode to which we are connected.
         * NameNode IDs are longs, so that's why this is of type long.
         */
        public long name = -1; // Hides super type.

        /**
         * Default constructor.
         */
        public NameNodeConnection() {

        }

        @Override
        public String toString() {
            return this.name != -1 ? String.valueOf(this.name) : super.toString();
        }
    }
}