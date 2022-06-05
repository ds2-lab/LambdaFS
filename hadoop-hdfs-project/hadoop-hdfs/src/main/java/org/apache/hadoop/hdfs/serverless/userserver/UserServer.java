package org.apache.hadoop.hdfs.serverless.userserver;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.exceptions.NoConnectionAvailableException;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessNameNodeClient;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResultWithMetrics;
import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessTcpUdpFuture;
import org.apache.hadoop.hdfs.serverless.execution.futures.ServerlessTcpUdpFuture;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.serverless.userserver.ServerlessClientServerUtilities.OPERATION_REGISTER;

/**
 * Clients of Serverless HopsFS expose a TCP server that serverless NameNodes can connect to.
 *
 * Clients will then issue TCP requests to the serverless NameNodes in order to perform file system operations.
 *
 * This is used on the client side (i.e., NOT on the NameNode side).
 */
public class UserServer {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(UserServer.class);

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
    private final ConcurrentHashMap<String, ServerlessTcpUdpFuture> activeFutures;

    /**
     * We also map the unique IDs of NameNodes to their deployments. This is used for debugging/logging and for
     * obtaining a connection to a NameNode from a specific deployment when invoking NNs.
     */
    private final ConcurrentHashMap<Long, Integer> nameNodeIdToDeploymentMapping;

    /**
     * A collection of all the futures that we've completed in one way or another (either they
     * were cancelled or we received a result for them).
     */
    private final Cache<String, ServerlessTcpUdpFuture> completedFutures;

    /**
     * Associate with each connection the list of futures that have been submitted and NOT completed.
     * The keys are NameNode IDs.
     *
     * If the connection is lost, then these futures must be re-submitted via HTTP.
     */
    private final ConcurrentHashMap<Long, List<ServerlessTcpUdpFuture>> submittedFutures;

    /**
     * Mapping of task/request ID to the NameNode to which the task/request was submitted.
     */
    private final ConcurrentHashMap<String, NameNodeConnection> futureToNameNodeMapping;

    /**
     * Implements all the Listener methods that we need. See the comment on the ServerListener class for more info.
     */
    private final ServerListener serverListener;

    /**
     * Indicates whether the TCP server should ultimately be started and enabled.
     */
    private final boolean enabled;

    /**
     * Indicates that the server has been started. Used to prevent multiple calls to the
     * {@link UserServer#startServer()} function from causing errors.
     */
    private boolean started;

    /**
     * Number of unique deployments.
     */
    private final int totalNumberOfDeployments;

    private static final Random rng = new Random();

    /**
     * We need a reference to this so that we can tell it what TCP port we ultimated bound to.
     */
    private final ServerlessNameNodeClient client;

    /**
     * Base buffer size. This is multiplied by {@code maxNumClients} to calculate the value of
     * {@code actualBufferSize}, which denotes the actual size of the buffers.
     */
    private final int baseBufferSize;

    private final int actualBufferSize;

    /**
     * If we're using straggler mitigation, then we may receive a response from a NN after our straggler
     * mitigation times out. If this happens, there probably won't be a RequestResponseFuture object registered
     * with the TCP server when the response comes in, so it just gets discarded, and then we get stuck in this
     * loop where we keep invoking and then timing out early and then missing the response. So, we hold onto these
     * responses so that, if the result gets re-submitted, we can return a result.
     */
    private final Cache<String, NameNodeResult> resultsWithoutFutures;

    /**
     * Use UDP instead of TCP.
     */
    private final boolean useUDP;

    private int udpPort;

    /**
     * Prefix used on debug messages. Of the form [CLIENT SERVER {TCP_PORT}] when in TCP-only mode and
     * [CLIENT SERVER {TCP_PORT}-{UDP_PORT}] when in TCP-UDP mode.
     */
    private String serverPrefix;

    /**
     * The maximum number of clients that may use this TCP server to submit FS operations to NameNodes.
     *
     * We use this value to determine how big to make the buffers.
     * Specifically, we multiply the base buffer size by this value.
     */
    // private final int maxNumClients;

    /**
     * Constructor.
     *
     * @param conf HopsFS configuration.
     * @param client The client using this TCP server.
     * @param tcpPort The TCP port to attempt to bind to.
     *                If this doesn't work, we'll try new ports by adding 1 to this value repeatedly until success.
     * @param udpPort The UDP port to attempt to bind to. Same situation as the {@code tcpPort} parameter.
     */
    public UserServer(Configuration conf, ServerlessNameNodeClient client, int tcpPort, int udpPort) {
        // COMMENTED OUT:
        // The TCP port is read from the config in the UserServerManager class and passed in.
        //this.tcpPort = conf.getInt(DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT,
        //        DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT_DEFAULT);
        //this.udpPort = conf.getInt(DFSConfigKeys.SERVERLESS_UDP_SERVER_PORT,
        //        DFSConfigKeys.SERVERLESS_UDP_SERVER_PORT_DEFAULT);
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.baseBufferSize = conf.getInt(SERVERLESS_TCP_BASE_BUFFER_SIZE, SERVERLESS_TCP_BASE_BUFFER_SIZE_DEFAULT);
        this.useUDP = conf.getBoolean(DFSConfigKeys.SERVERLESS_USE_UDP, DFSConfigKeys.SERVERLESS_USE_UDP_DEFAULT);
        // Set up state.
        this.allActiveConnections = new ConcurrentHashMap<>();
        this.submittedFutures = new ConcurrentHashMap<>();
        this.activeFutures = new ConcurrentHashMap<>();
        this.futureToNameNodeMapping = new ConcurrentHashMap<>();
        this.nameNodeIdToDeploymentMapping = new ConcurrentHashMap<>();
        this.activeConnectionsPerDeployment = new ConcurrentHashMap<>();
        //this.completedFutures = new ConcurrentHashMap<>();
        this.completedFutures = Caffeine.newBuilder()
                .maximumSize(2_500)
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
        this.resultsWithoutFutures = Caffeine.newBuilder()
                .maximumSize(1_000)
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
        this.client = client;

        int maxNumClients = conf.getInt(SERVERLESS_CLIENTS_PER_TCP_SERVER,
                SERVERLESS_CLIENTS_PER_TCP_SERVER_DEFAULT);
        int maxBufferSize = conf.getInt(SERVERLESS_TCP_MAX_BUFFER_SIZE, SERVERLESS_TCP_MAX_BUFFER_SIZE_DEFAULT);

        // If the configuration specifies a value <= 0, then all clients on this VM will use the same TCP server.
        // In this case, we just set the buffer size to the max size for both the "write buffer" and "object buffer".
//        if (maxNumClients <= 0) {
//            maxNumClients = Integer.MAX_VALUE;
//            this.actualBufferSize = maxBufferSize;
//        } else {
//            this.actualBufferSize = Math.min(maxBufferSize, maxNumClients * baseBufferSize);
//        }
        this.actualBufferSize = Math.min(maxBufferSize, baseBufferSize);

        LOG.info("User server will be used by at-most " + maxNumClients +
                (maxNumClients == 1 ? " client." : " clients.") + " Buffer size: " + actualBufferSize +
                " bytes each (there are 2 buffers).");

        // Read some options from config file.
        enabled = conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED,
                DFSConfigKeys.SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);
        totalNumberOfDeployments = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS,
                DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

        LOG.info("User server " + (enabled ? "ENABLED." : "DISABLED.") + " Running in " +
                (useUDP ? "TCP-UDP mode." : "TCP-only mode."));

        // Determine if TCP debug logging should be enabled.
        if (conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING,
                DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING_DEFAULT)) {
            LOG.debug("KryoNet Debug logging is ENABLED.");
            Log.set(Log.LEVEL_TRACE);
        }
        else
            LOG.debug("KryoNet Debug logging is DISABLED.");

        // Populate the active connections mapping with default, empty hash maps for each deployment.
        for (int deployNum = 0; deployNum < totalNumberOfDeployments; deployNum++) {
            activeConnectionsPerDeployment.put(deployNum, new ConcurrentHashMap<>());
        }

        // Create the TCP server.
        server = new Server(actualBufferSize, actualBufferSize) {
          /**
           * By providing our own connection implementation, we can store per-connection state
           * without a connection ID to perform state look-up.
           */
          @Override
          protected Connection newConnection() {
            LOG.debug(serverPrefix + " Creating new NameNodeConnection.");
            return new NameNodeConnection();
          }
        };

        // Create a listener object, which handles all listening events.
        serverListener = new ServerListener();

        // First, register the JsonObject class with the Kryo serializer.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(server.getKryo());

        server.addListener(serverListener);
    }

    /**
     * Stop the TCP server.
     */
    public void stop() {
        LOG.debug("HopsFSUserServer " + tcpPort + " stopping now...");
        this.server.removeListener(serverListener);
        LOG.debug("HopsFSUserServer " + tcpPort + " removed listener.");
        this.server.stop();
        LOG.debug("HopsFSUserServer " + tcpPort + " stopped successfully.");
    }

    public boolean isUdpEnabled() {
        return useUDP;
    }

    /**
     * Start the TCP server.
     *
     * @return The TCP port used by the server.
     */
    public synchronized int startServer() throws IOException {
        if (!enabled) {
            LOG.warn("TCP Server is NOT enabled. Server will NOT be started.");
            return -1;
        }

        if (started) {
            LOG.warn("TCP Server has already been started.");
            return tcpPort;
        }

        if (useUDP)
            LOG.debug("Starting HopsFS USER SERVER now. [TCP+UDP Mode]");
        else
            LOG.debug("Starting HopsFS USER SERVER now. [TCP Only Mode]");

        // Start the tcp/udp server.
        server.start();

        // Bind to the specified TCP port so the server listens on that port.
        int maxTcpPort = tcpPort + 999;
        int maxUdpPort = udpPort + 999;
        int currentTcpPort = tcpPort;
        int currentUdpPort = udpPort;
        boolean success = false;
        while (currentTcpPort < maxTcpPort && currentUdpPort < maxUdpPort && !success) {
            try {
                if (useUDP) {
                    LOG.debug("[USER SERVER] Trying to bind to TCP port " + currentTcpPort +
                            " and UDP port " + currentUdpPort + ".");
                    server.bind(currentTcpPort, currentUdpPort);
                } else {
                    LOG.debug("[USER SERVER] Trying to bind to TCP port " + currentTcpPort + ".");
                    server.bind(currentTcpPort);
                }

                if (tcpPort != currentTcpPort) {
                    LOG.warn("[USER SERVER] Configuration specified port " + tcpPort +
                            ", but we were unable to bind to that port. Instead, we are bound to port " + currentTcpPort +
                            ".");
                    this.tcpPort = currentTcpPort;
                }

                if (useUDP && udpPort != currentUdpPort) {
                    LOG.warn("[USER SERVER] Configuration specified port " + udpPort +
                            ", but we were unable to bind to that port. Instead, we are bound to port " +
                            currentUdpPort + ".");
                    this.udpPort = currentUdpPort;
                }

                if (useUDP) {
                    if (LOG.isDebugEnabled()) LOG.debug("Successfully bound to TCP port " + tcpPort + ", UDP port " + udpPort + ".");

                    serverPrefix = "[USER SERVER " + tcpPort + "-" + udpPort + "]";
                } else {
                    if (LOG.isDebugEnabled()) LOG.debug("Successfully bound to TCP port " + tcpPort + ".");
                    serverPrefix = "[USER SERVER " + tcpPort + "]";
                }

                success = true;
            } catch (BindException ex) {
                currentTcpPort++;
                currentUdpPort++;
            }
        }

        if (!success)
            throw new IOException("Failed to start TCP/UDP server. Could not successfully bind to any ports.");

        client.setTcpServerPort(tcpPort);
        client.setUdpServerPort(udpPort);

        this.started = true;

        return tcpPort;
    }

    /**
     * Register the remote serverless NameNode locally. This involves assigning a name to the connection
     * object as well as caching the active connection locally.
     * @param connection The connection to the serverless name node.
     * @param nameNodeId The unique ID of the NameNode.
     * @param deploymentNumber The deployment in which the NameNode is running.
     */
    private void registerNameNode(NameNodeConnection connection, int deploymentNumber, long nameNodeId) {
        if (LOG.isDebugEnabled()) LOG.debug("Registering connection to NameNode " + nameNodeId + " from deployment "
                + deploymentNumber);
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
                LOG.warn(serverPrefix + " Already have an ACTIVE conn to NameNode " + nameNodeId +
                        " (deployment #" + deploymentNumber + ".");
                //LOG.warn(serverPrefix + " Replacing old, ACTIVE conn to NameNode " + nameNodeId +
                //        " (deployment #" + deploymentNumber + ") with a new one...");

                //oldConnection.close();

                // Keep old connection until it gets closed.
                connection.close();
                return;
            } else {
                LOG.error(serverPrefix + " Already have a conn to NameNode " + nameNodeId + " (deployment #" +
                        deploymentNumber + "), but it is apparently no longer connected...");
                LOG.warn(serverPrefix + " Replacing old, now-disconnected conn to NameNode " + nameNodeId +
                        " (deployment #" + deploymentNumber + ") with the new one...");
            }
        } else if (LOG.isDebugEnabled()) {
            // We don't want to print this debug message along with the ones from the if-statement above, so
            // we put it in the else block. It isn't contradictory or anything, but it'd be redundant.
            LOG.debug(serverPrefix + " Successfully registered connection with NN " + nameNodeId +
                    " from deployment " + deploymentNumber);
        }

        allActiveConnections.put(nameNodeId, connection);
        activeConnectionsPerDeployment.get(deploymentNumber).put(nameNodeId, connection);
        nameNodeIdToDeploymentMapping.put(nameNodeId, deploymentNumber);
    }

    public int getNumActiveConnections() { return this.allActiveConnections.size(); }

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
     * Get a random TCP connection for a NameNode from the specified deployment.
     *
     * @param deploymentNumber The deployment for which a connection is desired.
     * @throws NoConnectionAvailableException If there are no connections available to the target deployment.
     *
     * @return a random, active connection if one exists. Otherwise, returns null.
     */
    private NameNodeConnection getRandomConnection(int deploymentNumber)
            throws NoConnectionAvailableException {
        ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                activeConnectionsPerDeployment.get(deploymentNumber);

        // Return a random NameNode connection.
        NameNodeConnection[] values = deploymentConnections.values().toArray(new NameNodeConnection[0]);

        // If there are no available connections, then we will return null to indicate that this is the case.
        if (values.length == 0)
            throw new NoConnectionAvailableException(serverPrefix + " Was about to issue " + (useUDP ? "UDP" : "TCP") +
                    " request to NameNode deployment " + deploymentNumber + ", but no such connections exist...");

        // If there's just one, don't bother with the RNG object. Just return the first available connection.
        if (values.length == 1)
            return values[0];

        return values[(rng.nextInt(values.length))];
    }

    /**
     * Return the TCP port being used by the server.
     * This should only be called after {@link UserServer#startServer()} has been called.
     * Otherwise, an IllegalStateException will be thrown.
     *
     * @throws IllegalStateException When the server has not yet been started.
     */
    public int getTcpPort() {
        if (!started)
            throw new IllegalStateException("User server has not been started yet. TCP port has not been assigned.");

        return this.tcpPort;
    }

    /**
     * Return the UDP port being used by the server.
     * This should only be called after {@link UserServer#startServer()} has been called.
     * Otherwise, an IllegalStateException will be thrown.
     *
     * @throws IllegalStateException When the server has not yet been started.
     */
    public int getUdpPort() {
        if (!started)
            throw new IllegalStateException("User server has not been started yet. UDP port has not been assigned.");

        return this.udpPort;
    }

    /**
     * Get a random TCP connection for a NameNode from the specified deployment.
     * Will not return a TCP connection to the NameNode with the given ID. If
     * that is the only TCP connection available for that deployment, then
     * this will just return null, thereby indicating that there are no TCP
     * connections available.
     *
     * This is used with the 'straggler mitigation' technique to try to re-submit
     * stragglers to a different NN than the one to which they were originally sent.
     *
     * @param deploymentNumber The deployment for which a connection is desired.
     * @param excludedNameNode NN who should not have its connections returned.
     *
     * @return a random, active connection if one exists. Otherwise, returns null.
     * @throws NoConnectionAvailableException If there are no connections available to the target deployment (aside
     * from possibly a connection to the excluded name node).
     */
    private NameNodeConnection getRandomConnection(int deploymentNumber, long excludedNameNode)
            throws NoConnectionAvailableException {
        ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                activeConnectionsPerDeployment.get(deploymentNumber);

        // Return a random NameNode connection.
        ArrayList<NameNodeConnection> values = new ArrayList<>();

        // Do not add the excluded NN to the set of connections from which we're randomly picking one.
        for (NameNodeConnection conn : deploymentConnections.values()) {
            if (excludedNameNode == conn.name)
                values.add(conn);
        }

        // If there are no available connections, then we will return null to indicate that this is the case.
        if (values.size() == 0) {
            LOG.warn("There are no other deployment #" + deploymentNumber +
                    " connections aside from the one to NN " + excludedNameNode);

            throw new NoConnectionAvailableException(serverPrefix + " There are no TCP/UDP connections to deployment " +
                    deploymentNumber + " except for possibly a connection to excluded NN " + excludedNameNode +
                    ". Cannot issue TCP/UDP request.");
        }

        // If there's just one, don't bother with the RNG object. Just return the first available connection.
        if (values.size() == 1)
            return values.get(0);

        return values.get(rng.nextInt(values.size()));
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
        NameNodeConnection connection = allActiveConnections.getOrDefault(nameNodeId, null);

        if (connection != null) {
            if (connection.isConnected()) {
                if (LOG.isDebugEnabled()) LOG.debug("Attempting to delete active connection with NN " + nameNodeId + ".");

                if (errorIfActive) {
                    throw new IllegalStateException(serverPrefix + " Connection to NN " + nameNodeId
                            + " was found to be active when trying to delete it.");
                }

                if (deleteIfActive) {
                    connection.close();
                    allActiveConnections.remove(nameNodeId);

                    // Remove from the mapping for the NN's specific deployment.
                    int deploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);
                    ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                            activeConnectionsPerDeployment.get(deploymentNumber);
                    deploymentConnections.remove(nameNodeId);

                    cancelActiveFutures(connection.name);

                    submittedFutures.remove(connection.name);

                    if (LOG.isDebugEnabled())
                        LOG.debug(serverPrefix + " Closed and removed connection to NN " + nameNodeId);
                    return true;
                } else {
                    if (LOG.isDebugEnabled())
                        LOG.debug(serverPrefix + " Cannot remove connection to NN " + nameNodeId
                                + " because it is still active " + "(and the override flag was not set to true).");
                    return false;
                }
            }
            else {
                if (LOG.isDebugEnabled()) LOG.debug("Attempting to delete inactive connection with NN " + nameNodeId + ".");
                allActiveConnections.remove(nameNodeId);

                // Remove from the mapping for the NN's specific deployment.
                int deploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);
                ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                        activeConnectionsPerDeployment.get(deploymentNumber);
                deploymentConnections.remove(nameNodeId);

                if (LOG.isDebugEnabled())
                    LOG.debug(serverPrefix + " Removed already-closed connection to NN " + nameNodeId);
                return true;
            }
        } else {
            LOG.warn(serverPrefix + " Cannot remove connection to NN " + nameNodeId +
                    ". No such connection exists!");
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
            throw new IllegalStateException("Mapping of NameNode IDs to associated TCP connections is null for deployment " +
                    deploymentNumber + ". Valid deployments: " + StringUtils.join(",", activeConnectionsPerDeployment.keySet()) + ".");

        return deploymentConnections.size() > 0;
    }

    /**
     * Check if there exists at least one connection to a NameNode in the specified deployment besides
     * a connection to the NameNode with the given ID.
     *
     * This is useful if we want to send a TCP request to the same deployment as the deployment of the NN with
     * the ID {@code excludedNameNodeId}, but we do not want to send the request to NN {@code excludedNameNodeId}.
     * This can occur if we previously sent a request to NN {@code excludedNameNodeId} that timed out.
     *
     * @param deploymentNumber The deployment to which we're asking if at least one connection exists.
     * @param excludedNameNodeId The ID of the NameNode whose we do not want to count for the purposes of checking
     *                           if a connection exists.
     * @return True if a connection currently exists, otherwise false.
     */
    public boolean connectionExists(int deploymentNumber, long excludedNameNodeId) {
        if (deploymentNumber == -1)
            return false;

        ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                activeConnectionsPerDeployment.get(deploymentNumber);

        if (deploymentConnections == null)
            throw new IllegalStateException("Mapping of NameNode IDs to associated TCP connections is null for deployment " +
                    deploymentNumber + ". Valid deployments: " + StringUtils.join(",", activeConnectionsPerDeployment.keySet()) + ".");

        // If we still have a connection to the specified NN, then we need to have at least one other connection.
        // If we do not stil have such a connection, then having any connections at all would suffice.
        if (deploymentConnections.containsKey(excludedNameNodeId))
            return deploymentConnections.size() > 1;
        else
            return deploymentConnections.size() > 0;
    }

    /**
     * Return true if the request identified by the given requestId is still active (i.e., we're still waiting on
     * the result for that future.)
     * @param requestId The ID of the task/request.
     * @return True if we're still waiting for the result for the specified task/request.
     */
    public boolean isFutureActive(String requestId) {
        return activeFutures.containsKey(requestId);
    }

    /**
     * Checks if there is an active connection established to the NameNode with the given ID.
     *
     * @param nameNodeId The ID of the NN for which we're querying the existence of a connection.
     * @return True if a connection currently exists, otherwise false.
     */
    public boolean connectionExists(long nameNodeId) {
        Connection tcpConnection = getConnection(nameNodeId);

        if (tcpConnection != null) {
            if (tcpConnection.isConnected())
                return true;

            LOG.warn("Found that connection to NN " + nameNodeId + " is NOT connected while checking" +
                    " if it exists. Removing it from the connection mapping...");
            // If the connection is NOT active, then we need to remove it from our cache of connections.
            deleteConnection(nameNodeId, false, true);
        }

        return false;
    }

    /**
     * Print debug information about TCP server.
     * @return Number of active TCP connections.
     */
    public int printDebugInformation(Set<Long> nnIds) {
        int numActiveConnections = allActiveConnections.size();
        StringBuilder msg = new StringBuilder("Num Active Connections: " + numActiveConnections + ", Active Futures: " +
                activeFutures.size() + ", Completed Futures: " + completedFutures.asMap().size() + ". ");

//        LOG.debug("========== TCP Server Debug Information ==========");
//        LOG.debug("CONNECTIONS:");
//        LOG.debug("     Number of active connections: " + allActiveConnections.size());
//        LOG.debug("     Connected to:");
        for (Map.Entry<Integer, ConcurrentHashMap<Long, NameNodeConnection>> entry : activeConnectionsPerDeployment.entrySet()) {
            int deploymentNumber = entry.getKey();
            ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections = entry.getValue();
            nnIds.addAll(deploymentConnections.keySet());
            msg.append("D ").append(deploymentNumber).append(": ").append(deploymentConnections.size()).append(", ");
//            LOG.debug("     Deployment #" + deploymentNumber + ": ");
//
//            if (deploymentConnections.size() == 0) {
//                LOG.debug("               No connections established");
//                continue;
//            }
//
//            ConcurrentHashMap.KeySetView<Long, NameNodeConnection> keySetView = deploymentConnections.keySet();
//            keySetView.forEach(funcName -> LOG.debug("               " + funcName));
        }
//        LOG.debug("FUTURES:");
//        LOG.debug("     Number of active futures: " + activeFutures.size());
//        LOG.debug("     Number of completed futures: " + completedFutures.asMap().size());
//        LOG.debug("==================================================");

        LOG.info(msg);
        return numActiveConnections;
    }

    /**
     * Register a RequestResponseFuture with the server. The server will post the NameNode's response for the
     * associated request to the Future.
     *
     * Checks for duplicate futures before registering it.
     *
     * @param associatedPayload The payload that we will be sending via TCP to the target NameNode.
     * @param nnId The NameNodeID of the target NN.
     *
     * @return A new {@link ServerlessTcpUdpFuture} object if one does not already exist.
     * Otherwise, returns the existing RequestResponseFuture object.
     */
    public ServerlessTcpUdpFuture registerRequestResponseFuture(TcpUdpRequestPayload associatedPayload, long nnId) {
        ServerlessTcpUdpFuture requestResponseFuture = activeFutures.getOrDefault(associatedPayload.getRequestId(), null);

        // TODO: Previously, this function also checked completedFutures before registering. Do we need to do this?
        if (requestResponseFuture == null) {
            requestResponseFuture = new ServerlessTcpUdpFuture(associatedPayload, nnId);
            activeFutures.put(requestResponseFuture.getRequestId(), requestResponseFuture);
        }

        return requestResponseFuture;
    }

    /**
     * Inform the TCP server that a future has been resolved via HTTP, and as such it should be moved
     * out of the active futures mapping.
     * @param requestId The ID of the request/task that was resolved via TCP.
     */
    public boolean deactivateFuture(String requestId) {
        ServerlessTcpUdpFuture future = activeFutures.remove(requestId);

        if (future != null) {
            completedFutures.put(requestId, future);

            if (futureToNameNodeMapping.containsKey(requestId)) {
                NameNodeConnection connection = futureToNameNodeMapping.get(requestId);

                // Remove this future from the submitted futures list associated with the connection,
                // if it exists.
                if (connection != null && submittedFutures.containsKey(connection.name)) {
                    List<ServerlessTcpUdpFuture> futures = submittedFutures.get(connection.name);
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
     * @param requestId The unique ID of the task/request.
     * @param tryToAvoidTargetingSameNameNode If true, then we try to avoid resubmitting this request to the same
     *                                        NameNode as before. This would only be set to 'true' when
     *                                        resubmitting a request that timed-out. We want to avoid sending it
     *                                        to the same NN at which the request previously timed-out.
     * @return A Future representing the eventual response from the NameNode.
     */
    public ServerlessTcpUdpFuture issueTcpRequest(int deploymentNumber, boolean bypassCheck,
                                            String requestId, TcpUdpRequestPayload payload,
                                            boolean tryToAvoidTargetingSameNameNode) throws IOException {
        if (!bypassCheck && !connectionExists(deploymentNumber)) {
            LOG.warn(serverPrefix + " Was about to issue " + (useUDP ? "UDP" : "TCP") +
                    " request to NameNode deployment " + deploymentNumber + ", but connection no longer exists...");
            return null;
        }

        // Send the TCP request to the NameNode.
        NameNodeConnection tcpConnection;

        // If we're trying to avoid a specific NameNode (as part of the 'straggler mitigation' technique), then we
        // will try to select a random TCP connection to another NameNode from the same deployment. If no such TCP
        // connections are available, then we will instead issue a request via HTTP.
        if (tryToAvoidTargetingSameNameNode && futureToNameNodeMapping.containsKey(requestId)) {
            // Check if we already have a mapping to another NN connection for this future.
            NameNodeConnection previousConnection = futureToNameNodeMapping.getOrDefault(requestId, null);

            // Avoid race in the case that the connection gets terminated between containsKey() returning
            // and us trying to retrieve the mapped connection from the futureToNameNodeMapping map.
            if (previousConnection != null) {
                // We only want to try to grab a connection if at least one other connection to this deployment exists.
                // If no other connections are available, then we raise an exception.
                if (connectionExists(deploymentNumber, previousConnection.name))
                    tcpConnection = getRandomConnection(deploymentNumber, previousConnection.name);
                else
                    throw new NoConnectionAvailableException(serverPrefix + " There are no TCP/UDP connections to deployment " +
                            deploymentNumber + " except for possibly a connection to excluded NN " + previousConnection.name +
                            ". Cannot issue TCP/UDP request.");
            } else {
                tcpConnection = getRandomConnection(deploymentNumber);
            }
        } else {
            tcpConnection = getRandomConnection(deploymentNumber);
        }

        // Make sure the connection variable is non-null.
        if (tcpConnection == null) {
            LOG.error(serverPrefix + " Was about to issue " + (useUDP ? "UDP" : "TCP") +
                    " request to NameNode deployment " + deploymentNumber + ", but connection no longer exists...");
            throw new NoConnectionAvailableException(serverPrefix + " Was about to issue " + (useUDP ? "UDP" : "TCP") +
                    " request to NameNode deployment " + deploymentNumber + ", but connection no longer exists...");
        }

        // Make sure the connection is active.
        if (!tcpConnection.isConnected()) {
            LOG.warn(serverPrefix + " Selected connection to NameNode " + tcpConnection.name + " is NOT connected...");

            // Delete the connection. If it is active, then we throw an error, as we expect it to not be active.
            deleteConnection(tcpConnection.name, false, true);

            throw new NoConnectionAvailableException(serverPrefix + " Selected connection to NameNode " +
                    tcpConnection.name + " is NOT connected...");
        }

        ServerlessTcpUdpFuture requestResponseFuture = registerRequestResponseFuture(payload, tcpConnection.name);

        // Make note of this future as being incomplete.
        List<ServerlessTcpUdpFuture> incompleteFutures = submittedFutures.computeIfAbsent(
                tcpConnection.name, k -> new ArrayList<>());

        incompleteFutures.add(requestResponseFuture);
        futureToNameNodeMapping.put(requestId, tcpConnection);

        long sendStart = System.nanoTime();

        int bytesSent;
        if (useUDP)
            bytesSent = tcpConnection.sendUDP(payload);
        else
            bytesSent = tcpConnection.sendTCP(payload);

        long sendEnd = System.nanoTime();

        if (LOG.isDebugEnabled()) {
            double sendDurationMs = ((sendEnd - sendStart) / 1.0e6);
            if (sendDurationMs < 10)
                LOG.debug("Sent " + bytesSent + " bytes via " + (useUDP ? "UDP" : "TCP") + " for request " +
                        requestId + " in " + sendDurationMs + " ms.");
            else
                LOG.warn("Sent " + bytesSent + " bytes via " + (useUDP ? "UDP" : "TCP") + " for request " +
                        requestId + " in " + sendDurationMs + " ms!");
        }

        return requestResponseFuture;
    }

    /**
     * Issue a TCP request to the given NameNode. Ths function will check to ensure that the connection exists
     * first before issuing the connection.
     *
     * This function then waits for a response from the NameNode to be returned.
     *
     * This should NOT be called from the main thread.
     * @param deploymentNumber The NameNode to issue a request to. If this is -1, then the TCP server will randomly
     *                         select a target deployment/NameNode from among all available, active connections.
     * @param bypassCheck Do not check if the connection exists.
     * @param requestId The unique ID of the task/request.
     * @param operationName The name of the FS operation we're performing.
     * @param payload The payload to send to the NameNode in the TCP request.
     * @param timeout If positive, then wait for future to resolve with a timeout.
     *                If zero, then this will return immediately if the future is not available.
     *                If negative, then block indefinitely, waiting for the future to resolve.
     * @param tryToAvoidTargetingSameNameNode If true, then we try to avoid resubmitting this request to the same
     *                                        NameNode as before. This would only be set to 'true' when resubmitting
     *                                        a request that timed-out. We want to avoid sending it to the same NN at
     *                                        which the request previously timed-out.
     * @return The response from the NameNode, or null if the request failed for some reason.
     */
    public Object issueTcpRequestAndWait(int deploymentNumber, boolean bypassCheck, String requestId,
                                         String operationName, TcpUdpRequestPayload payload, long timeout,
                                         boolean tryToAvoidTargetingSameNameNode)
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        if (deploymentNumber == -1) {
            // Randomly select an available connection. This is implemented using existing constructs, so it
            // is a little awkward. We have a mapping of ALL active NN connections from NN ID --> Connection, and
            // we have a mapping from NN ID --> Deployment Number. So, we randomly select a NN ID from the active
            // connection mapping, then we resolve the NN ID to the deployment number, and use that as the target
            // deployment. The NN ID we randomly select may not be the NN we actually issue a request to, as we
            // pass that NN's deployment number. If we have multiple connections for that deployment, we may
            // randomly pick a different connection from that deployment.

            // So, get the IDs of all NNs for which we have an active connections.
            Long[] activeNameNodeConnectionIDs = allActiveConnections.keySet().toArray(new Long[0]);

            // Randomly select an ID from among all the IDs.
            long nameNodeId = activeNameNodeConnectionIDs[rng.nextInt(activeNameNodeConnectionIDs.length)];

            // Resolve that ID to a deployment, and use that as the target deployment.
            deploymentNumber = nameNodeIdToDeploymentMapping.get(nameNodeId);
        }

        if (resultsWithoutFutures.asMap().containsKey(requestId)) {
            if (LOG.isDebugEnabled()) LOG.debug("Found result for request " + requestId +
                    "in ResultsWithoutFutures cache. Returning cached result.");
            NameNodeResult previouslyReceivedResult = resultsWithoutFutures.asMap().remove(requestId);

            // There could be a race where the cache entry expires after we've checked if it exists, but before
            // we remove it. So, we check to ensure it is non-null before posting the result.
            if (previouslyReceivedResult != null)
                return previouslyReceivedResult;
        }
        else if (completedFutures.asMap().containsKey(requestId)) {
            ServerlessTcpUdpFuture future = completedFutures.getIfPresent(requestId);
            if (future != null && future.isDone()) return future.get();
        }

        long startTime = System.nanoTime();
        ServerlessTcpUdpFuture requestResponseFuture = issueTcpRequest(
                deploymentNumber, bypassCheck, requestId, payload, tryToAvoidTargetingSameNameNode);

        double tcpSendDuration = (System.nanoTime() - startTime) / 1.0e6;
        if (tcpSendDuration > 50) {
            LOG.warn("TCP request " + requestId + " to NN " + requestResponseFuture.getTargetNameNodeId() +
                    "(deployment=" + deploymentNumber + ") took " + tcpSendDuration + " ms to send.");
        }
        else if (LOG.isTraceEnabled())
            LOG.trace("Issued TCP request in " + tcpSendDuration + " ms.");

        if (requestResponseFuture == null)
            throw new IOException("Issuing TCP request returned null instead of future. Must have been no connections.");

        if (timeout >= 0)
            return requestResponseFuture.get(timeout, TimeUnit.MILLISECONDS);
        else
            return requestResponseFuture.get();
    }

    /**
     * Handle a result received from a remote NameNode.
     * @param result The result we received from the remote NameNode.
     * @param connection The connection to the remote NameNode.
     */
    private void handleResult(NameNodeResult result, NameNodeConnection connection) {
        String requestId = result.getRequestId();

        ServerlessTcpUdpFuture future = activeFutures.getOrDefault(requestId, null);

        // If there is no future associated with this operation, then we have no means to return
        // the result back to the client who issued the file system operation.
        if (future == null) {
            // Only cache the future if it hasn't already been completed.
            if (!completedFutures.asMap().containsKey(requestId)) {
                LOG.error(serverPrefix + " TCP Server received response for request " + requestId +
                        ", but there is no associated future registered with the server.");
                resultsWithoutFutures.put(requestId, result);
            }

            return;
        }

        // T
        boolean success = future.postResultImmediate(result);

        if (!success)
            throw new IllegalStateException("Failed to post result to future " + future.getRequestId());

        // Update state pertaining to futures.
        completedFutures.put(requestId, future); // Do this first to prevent races.
        activeFutures.remove(requestId);

        List<ServerlessTcpUdpFuture> incompleteFutures = submittedFutures.get(connection.name);
        incompleteFutures.remove(future);

        if (LOG.isDebugEnabled()) {
            if (result instanceof NameNodeResultWithMetrics) {
                NameNodeResultWithMetrics resultWithMetrics = (NameNodeResultWithMetrics)result;
                int deploymentNumber = resultWithMetrics.getDeploymentNumber();
                long nameNodeId = resultWithMetrics.getNameNodeId();
                LOG.debug(serverPrefix + " Obtained result for request " + requestId +
                        " from NN " + nameNodeId + ", deployment " + deploymentNumber + ".");
            } else {
                LOG.debug(serverPrefix + " Obtained result for request " + requestId + ".");
            }
        }
    }

    /**
     * Cancel the active (i.e., unfinished) requests associated with a particular NameNode.
     *
     * This is used when the TCP connection to that NameNode is disconnected.
     *
     * @param nameNodeId The ID of the NameNode whose requests must be cancelled.
     */
    private void cancelActiveFutures(long nameNodeId) {
        List<ServerlessTcpUdpFuture> incompleteFutures = submittedFutures.get(nameNodeId);

        if (incompleteFutures == null) {
            if (LOG.isDebugEnabled()) LOG.debug(serverPrefix + " There were no futures associated with now-closed connection to NN " + nameNodeId);
            return;
        }

        LOG.warn(serverPrefix + " There were " + incompleteFutures.size()
                + " incomplete future(s) associated with now-terminated connection to NN " + nameNodeId +
                ". Cancelling the future(s) now.");

        // TODO: If we handled the logic of re-submitting the tasks here, then we could bundle all of the cancelled
        //       futures together into a single HTTP request if we ended up having to fall back to HTTP. Right now,
        //       when we do things individually, we end up issuing a unique HTTP request for each cancelled request.
        //       This can result in 64+ unique HTTP requests being issued per VM, which is too many for OpenWhisk to
        //       handle in the configurations we've been testing.
        //
        // TODO: Alternatively, we could implement batching on the HTTP side so that always occurs.

        // Cancel each of the futures.
        for (ServerlessTcpUdpFuture future : incompleteFutures) {
            if (LOG.isDebugEnabled()) LOG.debug("    " + serverPrefix + " Cancelling future " + future.getRequestId() + " for operation " + future.getOperationName());
            try {
                future.cancel(ServerlessNameNodeKeys.REASON_CONNECTION_LOST, true);
            } catch (InterruptedException ex) {
                LOG.error("Error encountered while cancelling future " + future.getRequestId()
                        + " for operation " + future.getOperationName() + ":", ex);
            }
        }

        submittedFutures.remove(nameNodeId);
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

    /**
     * Implements the various listener methods we need for the server. We create a class so that we can explicitly
     * instantiate it and then remove it when stopping the server so that we do not see any trailing "connection lost"
     * messages.
     */
    private class ServerListener extends Listener {
        /**
         * Listener handles connection establishment with remote NameNodes.
         */
        public void connected(Connection conn) {
            if (LOG.isDebugEnabled())
                LOG.debug(serverPrefix + " Connection established with remote NameNode at " + conn.getRemoteAddressTCP());
            conn.setKeepAliveTCP(5000);
            conn.setTimeout(20000);
        }

        /**
         * This listener handles receiving TCP/UDP messages from the name nodes.
         * @param conn The connection to the name node.
         * @param object The object that was sent by the name node to the client (us).
         */
        public void received(Connection conn, Object object) {
            NameNodeConnection connection = (NameNodeConnection)conn;

            // If we received a JsonObject, then add it to the queue for processing.
            if (object instanceof NameNodeResult) {
                NameNodeResult result = (NameNodeResult) object;
                handleResult(result, connection);
            }
            else if (object instanceof String) {
                JsonObject body = new JsonParser().parse((String)object).getAsJsonObject();

                int deploymentNumber = body.getAsJsonPrimitive(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER).getAsInt();
                long nameNodeId = body.getAsJsonPrimitive(ServerlessNameNodeKeys.NAME_NODE_ID).getAsLong();
                String operation = body.getAsJsonPrimitive("op").getAsString();

                // There are currently two different operations that a NameNode may perform.
                // The first is registration. This operation results in the connection to the NameNode
                // being cached locally by the client. The second operation is that of returning a result
                // of a file system operation back to the user.
                switch (operation) {
                    case OPERATION_REGISTER:
                        registerNameNode(connection, deploymentNumber, nameNodeId);
                        break;
                    default:
                        LOG.warn(serverPrefix + " Unknown operation received from NameNode " + nameNodeId +
                                ", Deployment #" + deploymentNumber + ": '" + operation + "'");
                }
            }
            else if (object instanceof FrameworkMessage.KeepAlive) {
                // The server periodically sends KeepAlive objects to prevent client from disconnecting.
            }
            else {
                LOG.warn(serverPrefix + " Received object of unexpected type from remote client " + connection +
                        " at " + connection.getRemoteAddressTCP() + ". Object type: " +
                        object.getClass().getSimpleName() + ".");
                LOG.warn("Unexpected object: " + object.toString());
            }
        }

        /**
         * For debugging purposes, check the status of a NN (i.e., is it alive or not) to which
         * an active TCP connection was dropped.
         * @param nnId The unique identifier of the NameNode.
         * @param deploymentNumber The NameNode's deployment.
         */
        private void checkStatusOfDisconnectedNameNode(long nnId, int deploymentNumber) {
            // Check the ZooKeeper cluster to determine if the NN to which we lost connection is still active.
            ZKClient zkClient = client.getZkClient();
            try {
                boolean alive = zkClient.checkIfNameNodeIsAlive(deploymentNumber, Long.toString(nnId));
                LOG.warn("NN " + nnId + " (dep=" + deploymentNumber + ") is " + (alive ? "STILL ALIVE." : "NO LONGER ALIVE."));
            } catch (Exception ex) {
                LOG.error("Encountered ZooKeeper exception while checking status of NN " + nnId +
                        " from deployment " + deploymentNumber + ":", ex);
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
                long nnId = connection.name;

                int mappedDeploymentNumber = nameNodeIdToDeploymentMapping.get(nnId);
                LOG.warn(serverPrefix + " Lost connection to NN " + nnId +
                        " from deployment #" + mappedDeploymentNumber);
                allActiveConnections.remove(nnId);

                ConcurrentHashMap<Long, NameNodeConnection> deploymentConnections =
                        activeConnectionsPerDeployment.get(mappedDeploymentNumber);
                deploymentConnections.remove(nnId);

                cancelActiveFutures(nnId);

                // Check the status of the NN whom we lost connection to in the future.
                // At the time of writing this, ZooKeeper is configured with a tick time of 1,000ms.
                // Sessions expire after two tick times, which is 2,000ms. Thus, we wait to check on
                // the status of the NameNode until after ZooKeeper would've detected that it stopped.
//                ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//                scheduler.schedule(() -> {
//                    checkStatusOfDisconnectedNameNode(nnId, mappedDeploymentNumber);
//                }, 2500, TimeUnit.MILLISECONDS);
            } else {
                InetSocketAddress address = conn.getRemoteAddressTCP();
                if (address == null)
                    LOG.warn(serverPrefix + " Lost connection to unregistered NameNode.");
                else
                    LOG.warn(serverPrefix + " Lost connection to unregistered NameNode at " + address);
            }
        }
    }
}