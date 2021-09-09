package org.apache.hadoop.hdfs.serverless.tcpserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.invoking.OpenWhiskInvoker;

import java.io.Serializable;
import java.util.UUID;

/**
 * Encapsulates the information needed to communicate with a Serverless HopsFS client/user via TCP.
 *
 * There is an important distinction between this class and ServerlessHopsFSClient:
 *
 * There is generally just one instance of this class (NameNodeTCPClient) per NameNode. This class handles the actual
 * networking/TCP operations on behalf of the NameNode. That is, it sends/receives messages to the HopsFS clients.
 *
 * The ServerlessHopsFSClient represents a particular client of HopsFS that we may be communicating with. There may be
 * several of these objects created on a single NameNode. Each time a new client begins interacting with HopsFS, the
 * NameNode may create an instance of ServerlessHopsFSClient to maintain state about that client.
 *
 * The NameNodeTCPClient uses the ServerlessHopsFSClient objects to keep track of who it is talking to.
 *
 * This is used on the NameNode side.
 */
public class ServerlessHopsFSClient implements Serializable {
    private static final Log LOG = LogFactory.getLog(ServerlessHopsFSClient.class);

    private static final long serialVersionUID = -7643002851925107079L;

    /**
     * Unique identifier of this particular client.
     */
    private final String clientId;

    /**
     * External IPv4 address of this client.
     */
    private final String clientIp;

    /**
     * The port in use by this client.
     */
    private final int clientPort;

    //////////////////
    //////METRICS/////
    //////////////////

    /**
     * Number of bytes received by the NameNode from this particular client.
     */
    private int bytesReceived;

    /**
     * Number of bytes sent to this client from the NameNode.
     */
    private int bytesSent;

    /**
     * Number of individual messages received by the NameNode from this particular client.
     */
    private int messagesReceived;

    /**
     * Number of individual messages sent to this client from the NameNode.
     */
    private int messagesSent;

    /**
     * Default constructor.
     */
    public ServerlessHopsFSClient() {
        LOG.warn("Default constructor used to create ServerlessHopsFSClient instance...");

        clientId = UUID.randomUUID().toString();
        clientIp = "127.0.0.1";
        clientPort = 80;
    }

    /**
     * Non-default constructor.
     * @param id Unique identifier of this particular client.
     * @param ip External IPv4 address of this client.
     * @param port The port in use by this client.
     */
    public ServerlessHopsFSClient(String id, String ip, int port) {
        clientId = id;
        clientIp = ip;
        clientPort = port;

        LOG.debug("Created new Serverless HopsFS Client object with ID = " + id + ", IP = " + ip
                + ", port = " + port);
    }

    public String getClientIp() { return clientIp; }

    public String getClientId() { return clientId; }

    public int getClientPort() { return clientPort; }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (obj instanceof ServerlessHopsFSClient) {
            ServerlessHopsFSClient other = (ServerlessHopsFSClient)obj;

            return this.clientId.equals(other.clientId) &&
                    this.clientIp.equals(other.clientIp) &&
                    this.clientPort == other.clientPort;
        }

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + ((clientId != null) ? 0 : clientId.hashCode());
        result = prime * result + ((clientIp != null) ? 0 : clientIp.hashCode());
        result = prime * result + clientPort;

        return result;
    }

    @Override
    public String toString() {
        return "ServerlessHopsFSClient[ID: " + clientId + ", IP: " + clientIp + ", Port: " + clientPort + "]";
    }
}
