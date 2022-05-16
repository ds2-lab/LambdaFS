package org.apache.hadoop.hdfs.serverless.tcpserver;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessNameNodeClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_CLIENTS_PER_TCP_SERVER;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_CLIENTS_PER_TCP_SERVER_DEFAULT;

public class UserServerManager {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(UserServerManager.class);

    /**
     * Maximum number of clients allowed to use a single TCP server.
     */
    private volatile int maxClientsPerServer;

    private static UserServerManager instance;

    /**
     * Map from server TCP port to the associated {@link UserServer} instance.
     */
    private final ConcurrentHashMap<Integer, UserServer> tcpPortToServerMapping;

    /**
     * Map from server TCP port to the number of clients it has assigned to it.
     */
    private final ConcurrentHashMap<Integer, Integer> serverClientCounts;

    /**
     * The configuration that was used to configure this instance. This will
     * also be passed to the constructor of each {@link UserServer} that
     * gets created.
     */
    private volatile Configuration conf;

    /**
     * Indicates whether this instance has been configured already. Every client on the same
     * VM will attempt to set the configuration, but only the first will have any effect.
     */
    private volatile boolean configured = false;

    /**
     * Retrieve the singleton instance, or create it if it does not exist.
     *
     * IMPORTANT: You must call {@link UserServerManager#setConfiguration(Configuration)} on the instance
     * returned by this function. If the singleton instance is being created for the first time, then it
     * will not be configured properly unless the {@link UserServerManager#setConfiguration(Configuration)}
     * is called on it.
     *
     * @return the singleton {@link UserServerManager} instance.
     */
    public synchronized static UserServerManager getInstance() {
        if (instance == null)
            instance = new UserServerManager();

        return instance;
    }

    /**
     * Set the configuration of the UserServerManager instance. If the instance has
     * already been configured by another thread, then this function returns immediately.
     * @param configuration Configuration to be applied to both this instance and every
     *                      {@link UserServer} that gets created.
     */
    public synchronized void setConfiguration(Configuration configuration) {
        if (configured) return;

        this.maxClientsPerServer = configuration.getInt(SERVERLESS_CLIENTS_PER_TCP_SERVER,
                SERVERLESS_CLIENTS_PER_TCP_SERVER_DEFAULT);

        // If the user has specified a value <= 0, then all clients on the same VM will share the same server.
        if (this.maxClientsPerServer <= 0) {
            this.maxClientsPerServer = Integer.MAX_VALUE;
        }

        this.conf = configuration;
        this.configured = true;
    }

    private UserServerManager() {
        tcpPortToServerMapping = new ConcurrentHashMap<>();
        serverClientCounts = new ConcurrentHashMap<>();
    }

    /**
     * Unregister a client with the TCP server identified by the given TCP port.
     * Basically just decrements the count associated with the server.
     *
     * @param tcpPort The TCP port of the server from which the client wants to unregister.
     *
     * @throws IllegalArgumentException If there is no count mapping for a server with the given TCP port.
     * @throws IllegalStateException If the user server with the given TCP port has a current count of 0.
     */
    public synchronized void unregisterClient(int tcpPort) {
        if (!serverClientCounts.containsKey(tcpPort))
            throw new IllegalArgumentException("There is presently no client count mapping for a TCP server with TCP port " + tcpPort);

        int currentCount = serverClientCounts.get(tcpPort);

        if (currentCount == 0)
            throw new IllegalStateException("The current client count for the User Server with TCP port " +
                    tcpPort + " is 0. Cannot unregister client.");

        serverClientCounts.put(tcpPort, currentCount - 1);
    }

    /**
     * Register a client with a TCP server. All that actually happens here is that
     * the client gets the TCP server that it will use for communicating with NameNodes.
     *
     * NOTE: If this function has to create a new TCP server, then it will call {@link UserServer#startServer()}
     * automatically, so that function does not need to be called again.
     *
     * @param client The client registering with us. The {@link ServerlessNameNodeClient} should call this
     *               function and pass "this" for the {@code client} parameter.
     *
     * @return The TCP server that this particular client should use.
     */
    public synchronized UserServer registerWithTcpServer(ServerlessNameNodeClient client)
            throws IOException {
        UserServer assignedServer;
        int oldNumClients = -1;
        int assignedPort = -1;

        // Search for
        for (Map.Entry<Integer, Integer> entry : serverClientCounts.entrySet()) {
            int tcpPort = entry.getKey();
            int numClients = entry.getValue();

            if (numClients < maxClientsPerServer) {
                LOG.debug("Assigning client to TCP Server " + tcpPort + ", which will now have " +
                        (numClients + 1) + " clients assigned to it.");

                oldNumClients = numClients;
                assignedPort = tcpPort;
                break;
            }
        }

        if (oldNumClients == -1 && assignedPort == -1) {
            // Create new TCP server.
            LOG.debug("Creating new user server...");
            assignedServer = new UserServer(conf, client);
            int tcpPort = assignedServer.startServer();

            serverClientCounts.put(tcpPort, 1);
            tcpPortToServerMapping.put(tcpPort, assignedServer);

            LOG.debug("Created new TCP server with TCP port " + tcpPort + ". There are now " +
                    tcpPortToServerMapping.size() + " unique TCP server(s).");
        } else {
            LOG.debug("Retrieving existing TCP server with TCP port " + assignedPort + ".");
            // Grab existing server and return it.
            assignedServer = tcpPortToServerMapping.get(assignedPort);
            serverClientCounts.put(assignedPort, oldNumClients + 1);

            // Make sure to set the client's TCP/UDP port values, since this step
            // doesn't happen automatically when we're using an existing server.
            client.setTcpServerPort(assignedServer.getTcpPort());
            client.setUdpServerPort(assignedServer.getUdpPort());
        }

        return assignedServer;
    }
}
