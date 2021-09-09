package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.google.gson.JsonObject;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Encapsulates a Kryonet TCP client. Used to communicate directly with Serverless HopsFS clients.
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

        Instant connectStart = Instant.now();
        tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getClientPort());
        Instant connectEnd = Instant.now();

        Duration connectDuration = Duration.between(connectStart, connectEnd);
        double connectMilliseconds = ((double)connectDuration.getNano() / 1000.0) +
                ((double)connectDuration.getSeconds() * 1000);

        LOG.debug("Successfully established connection with client " + newClient.getClientId()
                + " in " + connectMilliseconds + " milliseconds!");

        // We need to register whatever classes will be serialized BEFORE any network activity is performed.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(tcpClient.getKryo());

        tcpClient.addListener(new Listener() {
            public void received(Connection connection, Object object) {
                LOG.debug("Received message from connection " + connection.toString());

                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof JsonObject) {
                    JsonObject args = (JsonObject)object;

                    LOG.debug("Message contents: " + args.toString());

                    // TODO: Extract desired FS operation from message, add it to worker thread queue,
                    //       then block on task future and return result to client when it resolves.
                }
                else {
                    throw new IllegalArgumentException("Received object of unexpected type from client "
                            + tcpClient.toString() + ". Object type: " + object.getClass().getSimpleName() + ".");
                }
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

    /**
     * @return The number of active TCP clients connected to the NameNode.
     */
    public int numClients() {
        return tcpClients.size();
    }

    /*public int workQueueSize() {
        return workQueue.size();
    }

    *//**
     * Return the work queue object (which is of type ConcurrentLinkedQueue<JsonObject>).
     * @return the work queue object (which is of type ConcurrentLinkedQueue<JsonObject>).
     *//*
    public ConcurrentLinkedQueue<JsonObject> getWorkQueue() {
        return workQueue;
    }*/
}
