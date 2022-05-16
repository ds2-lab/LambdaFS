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
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(UserTcpUdpServer.class);

    /**
     * Maximum number of clients allowed to use a single TCP server.
     */
    private int maxClientsPerServer;

    private static UserServerManager instance;

    /**
     * Map from server TCP port to the associated {@link UserTcpUdpServer} instance.
     */
    private final Map<Integer, UserTcpUdpServer> tcpPortToServerMapping;

    /**
     * Map from server TCP port to the number of clients it has assigned to it.
     */
    private final Map<Integer, Integer> tcpServerClientCounts;

    public UserServerManager getInstance() {
        if (instance == null)
            instance = new UserServerManager();

        return instance;
    }

    public void setConfiguration(Configuration configuration) {
        maxClientsPerServer = configuration.getInt(SERVERLESS_CLIENTS_PER_TCP_SERVER,
                SERVERLESS_CLIENTS_PER_TCP_SERVER_DEFAULT);
    }

    private UserServerManager() {
        tcpPortToServerMapping = new ConcurrentHashMap<>();
        tcpServerClientCounts = new ConcurrentHashMap<>();
    }

    /**
     * Register a client with a TCP server. All that actually happens here is that
     * the client gets the TCP server that it will use for communicating with NameNodes.
     *
     * @param client The client registering with us. The {@link ServerlessNameNodeClient} should call this
     *               function and pass "this" for the {@code client} parameter.
     * @param conf The configuration being used by the client. Will be passed to the TCP server.
     *
     * @return The TCP server that this particular client should use.
     */
    public synchronized UserTcpUdpServer registerWithTcpServer(ServerlessNameNodeClient client, Configuration conf)
            throws IOException {
        UserTcpUdpServer assignedServer = null;
        int oldNumClients = -1;
        int assignedPort = -1;

        for (Map.Entry<Integer, Integer> entry : tcpServerClientCounts.entrySet()) {
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
            LOG.debug("Creating new TCP server.");
            assignedServer = new UserTcpUdpServer(conf, client);
            int tcpPort = assignedServer.startServer();

            tcpServerClientCounts.put(tcpPort, 1);
            tcpPortToServerMapping.put(tcpPort, assignedServer);
        } else {
            // Grab existing server and return it.
            assignedServer = tcpPortToServerMapping.get(assignedPort);
            tcpServerClientCounts.put(assignedPort, oldNumClients + 1);
        }

        return assignedServer;
    }
}
