package org.apache.hadoop.hdfs.serverless.userserver;

/**
 * Encapsulates the information needed to communicate with a Serverless HopsFS client/user via TCP.
 *
 * There is an important distinction between this class and ServerlessHopsFSClient:
 *
 * There is generally just one instance of the NameNodeTCPClient class per NameNode. The NameNodeTCPClient class handles
 * the actual networking/TCP operations on behalf of the NameNode. That is, it sends/receives messages to the HopsFS
 * clients.
 *
 * The ServerlessHopsFSClient class represents a particular client of HopsFS that we may be communicating with.
 * There may be several of these objects created on a single NameNode. Each time a new client begins interacting with
 * HopsFS, the NameNode may create an instance of ServerlessHopsFSClient to maintain state about that client.
 *
 * The NameNodeTCPClient uses the ServerlessHopsFSClient objects to keep track of who it is talking to.
 *
 * This is used on the NameNode side.
 */
public class ServerlessHopsFSClient {
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
    private final int tcpPort;

    /**
     * Port used for the UDP server.
     */
    private final int udpPort;

    private final boolean udpEnabled;

    /**
     * Non-default constructor.
     * @param id Unique identifier of this particular client.
     * @param ip External IPv4 address of this client.
     * @param tcpPort The port in use by this client.
     * @param udpPort The port to use to connect to the UDP server.
     * @param udpEnabled Indicates whether the server is running in TCP+UDP mode (true) or TCP-only mode (false).
     */
    public ServerlessHopsFSClient(String id, String ip, int tcpPort, int udpPort, boolean udpEnabled) {
        this.clientId = id;
        this.clientIp = ip;
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.udpEnabled = udpEnabled;
    }

    public String getClientIp() { return clientIp; }

    public String getClientId() { return clientId; }

    public int getTcpPort() { return tcpPort; }

    public int getUdpPort() { return udpPort; }

    public boolean getUdpEnabled() { return udpEnabled; }

    public String getTcpString() { return clientIp + ":" + tcpPort; }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (obj instanceof ServerlessHopsFSClient) {
            ServerlessHopsFSClient other = (ServerlessHopsFSClient)obj;

            return this.clientId.equals(other.clientId) &&
                    this.clientIp.equals(other.clientIp) &&
                    this.tcpPort == other.tcpPort;
        }

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + ((clientId != null) ? clientId.hashCode() : 0);
        result = prime * result + ((clientIp != null) ? clientIp.hashCode() : 0);
        result = prime * result + tcpPort;

        return result;
    }

    @Override
    public String toString() {
        return "ServerlessHopsFSClient(ID: " + clientId + ", IP: " + clientIp + ", Port: " + tcpPort + ")";
    }
}
