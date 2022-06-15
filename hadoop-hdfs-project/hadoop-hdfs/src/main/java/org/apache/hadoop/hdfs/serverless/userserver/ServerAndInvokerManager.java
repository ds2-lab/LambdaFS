package org.apache.hadoop.hdfs.serverless.userserver;

import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerBase;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerFactory;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessNameNodeClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class ServerAndInvokerManager {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ServerAndInvokerManager.class);

    /**
     * Maximum number of clients allowed to use a single TCP server.
     */
    private volatile int maxClientsPerServer;

    private static ServerAndInvokerManager instance;

    /**
     * Map from server TCP port to the associated {@link UserServer} instance.
     */
    private final HashMap<Integer, UserServer> tcpPortToServerMapping;

    /**
     * Mapping from TCP port to {@link ServerlessInvokerBase} instance.
     */
    private final HashMap<Integer, ServerlessInvokerBase> tcpPortToInvokerMapping;

    /**
     * Map from server TCP port to the number of clients it has assigned to it.
     */
    private final HashMap<Integer, Integer> serverClientCounts;

    /**
     * Set of all TCP ports actively in-use by a server running within this JVM.
     */
    private Set<Integer> activeTcpPorts;

    /**
     * Set of all TCP ports actively in-use by a server running within this JVM.
     */
    private Set<Integer> activeUdpPorts;

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
     * Next TCP port to use when creating a TCP server.
     */
    private volatile int nextTcpPort;

    /**
     * Next TCP port to use when creating a TCP server.
     */
    private volatile int nextUdpPort;

    /**
     * Name of the serverless platform we're using.
     */
    private String serverlessPlatformName;

    private final ReadWriteLock mutex = new ReentrantReadWriteLock();

    private final ArrayList<UserServer> userServers = new ArrayList<>();

    private final ArrayList<ServerlessInvokerBase> invokers = new ArrayList<>();

    /**
     * Indicates whether the {@link ServerAndInvokerManager#terminate()} function has been called.
     *
     * TODO: Should we check if `stopped` is true before doing work in most functions?
     */
    private volatile boolean stopped = false;

    /**
     * Gets incremented every time somebody calls {@link ServerAndInvokerManager#findServerWithActiveConnectionToDeployment(int)}
     * or {@link ServerAndInvokerManager#findServerWithAtLeastOneActiveConnection()}. We use this to determine when to shuffle
     * the list of user servers to ensure servers at the beginning of the list aren't disproportionately used.
     */
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * How frequently we shuffle the list of user servers.
     *
     * TODO: Should this also be volatile? It's only written to a single time.
     */
    private int shuffleEvery;

    /**
     * Retrieve the singleton instance, or create it if it does not exist.
     *
     * IMPORTANT: You must call {@link ServerAndInvokerManager#setConfiguration(Configuration)} on the instance
     * returned by this function. If the singleton instance is being created for the first time, then it
     * will not be configured properly unless the {@link ServerAndInvokerManager#setConfiguration(Configuration)}
     * is called on it.
     *
     * @return the singleton {@link ServerAndInvokerManager} instance.
     */
    public synchronized static ServerAndInvokerManager getInstance() {
        if (instance == null)
            instance = new ServerAndInvokerManager();

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

        this.shuffleEvery = configuration.getInt(SERVERLESS_SHUFFLE_SERVERS_EVERY,
                SERVERLESS_SHUFFLE_SERVERS_EVERY_DEFAULT);

        this.nextTcpPort = configuration.getInt(SERVERLESS_TCP_SERVER_PORT, SERVERLESS_TCP_SERVER_PORT_DEFAULT);
        this.nextUdpPort = configuration.getInt(DFSConfigKeys.SERVERLESS_UDP_SERVER_PORT,
                DFSConfigKeys.SERVERLESS_UDP_SERVER_PORT_DEFAULT);

        this.serverlessPlatformName = configuration.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT);

        // If the user has specified a value <= 0, then all clients on the same VM will share the same server.
        if (this.maxClientsPerServer <= 0) {
            this.maxClientsPerServer = Integer.MAX_VALUE;
        }

        this.conf = configuration;
        this.configured = true;
    }

    private ServerAndInvokerManager() {
        tcpPortToServerMapping = new HashMap<>();
        tcpPortToInvokerMapping = new HashMap<>();
        serverClientCounts = new HashMap<>();
        activeTcpPorts = new HashSet<>();
        activeUdpPorts = new HashSet<>();
    }

    /**
     * Find and return a {@link UserServer} that has an active TCP/UDP connection to the specified deployment, or null
     * if no such server exists.
     *
     * @param targetDeployment The deployment to which the returned server should have an active TCP/UDP connection.
     * @return A {@link UserServer} with an active TCP/UDP connection to the target deployment, or null if no such
     * servers exist.
     */
    public UserServer findServerWithActiveConnectionToDeployment(int targetDeployment) {
        checkIfShuffleRequired();

        mutex.readLock().lock();
        try {
            for (UserServer userServer : userServers) {
                if (userServer.connectionExists(targetDeployment))
                    return userServer;
            }

            return null;
        } finally {
            mutex.readLock().unlock();
        }
    }

    /**
     * Check if we should shuffle the {@link ServerAndInvokerManager#userServers} variable. If so, then shuffle it.
     */
    private void checkIfShuffleRequired() {
        int val = counter.incrementAndGet();

        if (val % shuffleEvery == 0) {
            mutex.writeLock().lock();
            try {
                Collections.shuffle(userServers);
            } finally {
                mutex.writeLock().unlock();
            }
        }
    }

    /**
     * Find and return a server that has at least one active TCP connection.
     */
    public UserServer findServerWithAtLeastOneActiveConnection() {
        checkIfShuffleRequired();

        mutex.readLock().lock();
        try {
            for (UserServer userServer : userServers) {
                if (userServer.getNumActiveConnections() > 0)
                    return userServer;
            }

            return null;
        } finally {
            mutex.readLock().unlock();
        }
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
    // synchronized so multiple calls to this function cannot overlap.
    // we also use locking so that the `findServerWithActiveConnectionToDeployment()` can execute concurrently,
    // although this probably shouldn't happen due to when these synchronized functions are called in a workload.
    public synchronized void unregisterClient(int tcpPort) {
        mutex.writeLock().lock();
        try {
            if (!serverClientCounts.containsKey(tcpPort))
                throw new IllegalArgumentException("There is presently no client count mapping for a TCP server with TCP port " + tcpPort);

            int currentCount = serverClientCounts.get(tcpPort);

            if (currentCount == 0)
                throw new IllegalStateException("The current client count for the User Server with TCP port " +
                        tcpPort + " is 0. Cannot unregister client.");

            serverClientCounts.put(tcpPort, currentCount - 1);
        } finally {
            mutex.writeLock().unlock();
        }
    }

    /**
     * Call {@link UserServer#printDebugInformation(Set)} )} on each of our servers.
     *
     * This also shuffles the list of all user servers.
     *
     * @return The total number of active TCP connections across all servers.
     */
    public int printDebugInformation() {
        int numActiveConnections = 0;

        mutex.readLock().lock();
        try {
            LOG.info("There are " + tcpPortToServerMapping.values().size() + " TCP servers.");
            Set<Long> nnIds = new HashSet<Long>();
            for (UserServer server : tcpPortToServerMapping.values())
                numActiveConnections += server.printDebugInformation(nnIds);

            LOG.info("Clients in this JVM are connected to a total of " + nnIds.size() + " unique NNs.");
        } finally {
            mutex.readLock().unlock();
        }

        mutex.writeLock().lock();
        try {
            Collections.shuffle(userServers);
        } finally {
            mutex.writeLock().unlock();
        }

        return numActiveConnections;
    }

    /**
     * Return a copy of the set of all actively in-use TCP ports.
     */
    public List<Integer> getActiveTcpPorts() {
        mutex.readLock().lock();
        try{
            return new ArrayList<>(activeTcpPorts);
        } finally {
            mutex.readLock().unlock();
        }
    }

    /**
     * Return a copy of the set of all actively in-use UDP ports.
     */
    public List<Integer> getActiveUdpPorts() {
        mutex.readLock().lock();
        try{
            return new ArrayList<>(activeUdpPorts);
        } finally {
            mutex.readLock().unlock();
        }
    }

    /**
     * Terminate all the {@link UserServer} and {@link ServerlessInvokerBase} instances.
     */
    public synchronized void terminate() {
        if (!stopped) {
            stopped = true;

            for (UserServer userServer : userServers)
                userServer.terminate();

            for (ServerlessInvokerBase invoker : invokers)
                invoker.terminate();
        }
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
     * @param clientName Passed to new invokers.
     * @param serverlessEndpointBase Passed to new invokers.
     *
     * @return The TCP server that this particular client should use.
     */
    // synchronized so multiple calls to this function cannot overlap.
    // we also use locking so that the `findServerWithActiveConnectionToDeployment()` can execute concurrently,
    // although this probably shouldn't happen due to when these synchronized functions are called in a workload.
    public synchronized Pair<UserServer, ServerlessInvokerBase> registerClient(
            ServerlessNameNodeClient client, String clientName, String serverlessEndpointBase) throws IOException {
        if (stopped)
            throw new IllegalStateException("Manager has been stopped. Cannot register new clients.");

        UserServer assignedServer;
        ServerlessInvokerBase assignedInvoker;
        int oldNumClients = -1;
        int assignedPort = -1;

        mutex.writeLock().lock();
        try {
            // Search for server with empty slots.
            for (Map.Entry<Integer, Integer> entry : serverClientCounts.entrySet()) {
                int tcpPort = entry.getKey();
                int numClients = entry.getValue();

                if (numClients < maxClientsPerServer) {
                    LOG.info("Assigning client " + clientName + " to TCP Server " + tcpPort +
                            ", which will now have " + (numClients + 1) + " clients assigned to it.");

                    oldNumClients = numClients;
                    assignedPort = tcpPort;
                    break;
                }
            }

            if (oldNumClients == -1 && assignedPort == -1) {
                // Create new TCP server.
                LOG.info("Creating new user server for client " + clientName + ". Attempting to use TCP port " +
                        nextTcpPort + ", UDP port " + nextUdpPort);
                assignedServer = new UserServer(conf, client, nextTcpPort, nextUdpPort);
                assignedInvoker = ServerlessInvokerFactory.getServerlessInvoker(serverlessPlatformName);
                assignedInvoker.setIsClientInvoker(true);
                assignedInvoker.setConfiguration(conf, "C-" + clientName, serverlessEndpointBase);
                int tcpPort = assignedServer.startServer();
                assert(tcpPort == assignedPort);

                activeTcpPorts.add(tcpPort);

                if (assignedServer.isUdpEnabled() && client.isTcpEnabled())
                    activeUdpPorts.add(assignedServer.getUdpPort());

                // We now set the ports here.
                assignedInvoker.setTcpPort(assignedPort);

                if (assignedServer.isUdpEnabled() && client.isTcpEnabled())
                    assignedInvoker.setUdpPort(assignedServer.getUdpPort());

                assignedInvoker.setServerlessFunctionLogLevel(client.getServerlessFunctionLogLevel());
                assignedInvoker.setConsistencyProtocolEnabled(client.getConsistencyProtocolEnabled());
                assignedInvoker.setBenchmarkModeEnabled(client.getBenchmarkModeEnabled());

                serverClientCounts.put(tcpPort, 1);
                tcpPortToServerMapping.put(tcpPort, assignedServer);
                tcpPortToInvokerMapping.put(tcpPort, assignedInvoker);
                nextTcpPort++;
                nextUdpPort++;

                LOG.info("Created new TCP server with TCP port " + tcpPort + " for client " + clientName +
                        ". There are now " + tcpPortToServerMapping.size() + " unique TCP server(s).");

                userServers.add(assignedServer);
                invokers.add(assignedInvoker);
            } else {
                LOG.info("Retrieving existing TCP server with TCP port " + assignedPort + " for client " +
                        clientName + ".");
                // Grab existing server and return it.
                assignedServer = tcpPortToServerMapping.get(assignedPort);
                assignedInvoker = tcpPortToInvokerMapping.get(assignedPort);
                serverClientCounts.put(assignedPort, oldNumClients + 1);

                // COMMENTED OUT: The TCP/UDP port will be assigned when the invoker is first created.
                // Don't need to do it again each time we assign a new client to the invoker.

                // Make sure to set the client's TCP/UDP port values, since this step
                // doesn't happen automatically when we're using an existing server.
                // client.setTcpServerPort(assignedServer.getTcpPort());
                // client.setUdpServerPort(assignedServer.getUdpPort());
            }
        } finally {
            mutex.writeLock().unlock();
        }

        assert(assignedServer != null);
        assert(assignedInvoker != null);
        return new Pair<>(assignedServer, assignedInvoker);
    }

//    public void clearStatisticsPackages() {
//        for (ServerlessInvokerBase invokerBase : invokers) {
//            invokerBase.getStatisticsPackages().clear();
//        }
//    }
//
//    public void clearTransactionStatistics() {
//        for (ServerlessInvokerBase invokerBase : invokers) {
//            invokerBase.getTransactionEvents().clear();
//        }
//    }
//    public void mergeStatisticsPackages(HashMap<String, TransactionsStats.ServerlessStatisticsPackage> packages,
//                                        boolean keepLocal) {
//        for (ServerlessInvokerBase invokerBase : invokers) {
//            invokerBase.getTransactionEvents().clear();
//        }
//    }
}
