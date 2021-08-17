package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Encapsulates a Kryonet TCP client. Used to communicate directly with Serverless HopsFS clients.
 */
public class NameNodeTCPClient {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(NameNodeTCPClient.class);

    /**
     * This is the maximum amount of time a call to connect() will block. Calls to connect() occur when
     * establishing a connection to a new client.
     */
    private static final int CONNECTION_TIMEOUT = 5000;

    /**
     * Mapping from instances of ServerlessHopsFSClient to their associated TCP client object.
     */
    private final HashMap<ServerlessHopsFSClient, Client> tcpClients;

    public NameNodeTCPClient() {
        tcpClients = new HashMap<>();
    }

    /**
     * Register a new Serverless HopsFS client with the NameNode and establish a TCP connection with the new client.
     * @param newClient The new Serverless HopsFS client.
     * @return true if the connection was established successfully,
     * false if a connection with that client already exists.
     * @throws IOException If the connection to the new client times out.
     */
    public boolean addClient(ServerlessHopsFSClient newClient) throws IOException {
        if (tcpClients.containsKey(newClient)) {
            LOG.warn("NameNodeTCPClient already has a connection to client " + newClient);
            return false;
        }

        LOG.debug("Establishing connection with new Serverless HopsFS client " + newClient + " now...");

        // The call to connect() may produce an IOException if it times out.
        Client tcpClient = new Client();
        tcpClient.start();
        tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getClientPort());

        tcpClient.addListener(new Listener() {
            public void received(Connection connection, Object object) {
                LOG.debug("Received message from connection " + connection.toString());
            }
        });

        tcpClients.put(newClient, tcpClient);

        return true;
    }

    /**
     * Unregister the given HopsFS client with the NameNode. Terminates the TCP connection to this client.
     * @param client The HopsFS client to unregister.
     * @return true if unregistered successfully, false if the client was already not registered with the NameNode.
     */
    public boolean removeClient(ServerlessHopsFSClient client) {
        Client tcpClient = tcpClients.getOrDefault(client, null);

        if (tcpClient == null) {
            LOG.warn("No TCP client associated with HopsFS client " + client.toString());
            return false;
        }

        // Stop the TCP client. This function calls the close() method.
        tcpClient.stop();

        tcpClients.remove(client);

        return true;
    }
}
