package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.github.benmanes.caffeine.cache.*;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.BaseHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments.HashMapTaskArguments;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResultWithMetrics;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.serverless.OpenWhiskHandler.getLogLevelFromInteger;

/**
 * Encapsulates a Kryonet TCP client. Used to communicate directly with Serverless HopsFS clients.
 *
 * There is an important distinction between this class and ServerlessHopsFSClient:
 *
 * There is generally just one instance of this class (NameNodeTCPClient) per NameNode. This class handles the actual
 * networking/TCP operations on behalf of the NameNode. That is, it sends/receives messages to the HopsFS clients.
 *
 * The ServerlessHopsFSClient class represents a particular client of HopsFS that we may be communicating with. There
 * may be several of these objects created on a single NameNode. Each time a new client begins interacting with HopsFS,
 * the NameNode may create an instance of ServerlessHopsFSClient to maintain state about that client.
 *
 * The NameNodeTCPClient uses the ServerlessHopsFSClient objects to keep track of who it is talking to.
 *
 * This is used on the NameNode side.
 */
public class NameNodeTcpUdpClient {
    private static final Log LOG = LogFactory.getLog(NameNodeTcpUdpClient.class);

    /**
     * This is the maximum amount of time a call to connect() will block. Calls to connect() occur when
     * establishing a connection to a new client.
     */
    private static final int CONNECTION_TIMEOUT = 8000;

    /**
     * Mapping from instances of ServerlessHopsFSClient to their associated TCP/UDP client object. Recall that each
     * ServerlessHopsFSClient represents a particular client of HopsFS that the NameNode is talking to. We map
     * each of these "HopsFS client" representations to the TCP/UDP Client associated with them.
     */
    private final Cache<ServerlessHopsFSClient, Client> clients;

    /**
     * The fraction of main memory reserved for TCP/UDP connection buffers.
     */
    private static final float memoryFractionReservedForTcpBuffers = 0.13f;

    /**
     * The deployment number of the local serverless name node instance.
     */
    private final int deploymentNumber;

    /**
     * The unique ID of the local serverless name node instance.
     */
    private final long nameNodeId;

    /**
     * The ServerlessNameNode instance that this TCP Client is associated with.
     */
    private final ServerlessNameNode serverlessNameNode;

    /**
     * The size, in bytes, used for the write buffer of new TCP connections. Objects are serialized to
     * the write buffer where the bytes are queued until they can be written to the TCP socket.
     */
    private static final int defaultWriteBufferSizeBytes = (int)2.5e5;

    /**
     * The size, in bytes, used for the object buffer of new TCP connections. Object buffers are used
     * to hold the bytes for a single object graph until it can be sent over the network or deserialized.
     */
    private static final int defaultObjectBufferSizeBytes = (int)2.5e5;

    /**
     * The maximum size, in bytes, that can be used for a TCP write buffer or a TCP object buffer.
     *
     * If we find that we're trying to write data that is larger than the buffer(s), then we change the
     * size of the buffers for future TCP connections to hopefully avoid the problem. This variable sets a hard limit
     * on the maximum size of a buffer.
     */
    private static final int maxBufferSize = (int)5e5;

    /**
     * The current size, in bytes, being used for TCP write buffers. If we notice a buffer overflow,
     * then we increase the size of this buffer for future TCP connections in the hopes that we'll
     * avoid a buffer overflow.
     */
    private final int writeBufferSize;

    /**
     * The current size, in bytes, being used for TCP object buffers. If we notice a buffer overflow,
     * then we increase the size of this buffer for future TCP connections in the hopes that we'll
     * avoid a buffer overflow.
     */
    private int objectBufferSize;

    /**
     * The amount of RAM (in megabytes) that this function has been allocated. Used when determining the number of active
     * TCP connections that this NameNode can have at once, as each TCP connection has two relatively-large buffers. If
     * the NN creates too many TCP connections at once, then it might crash due to OOM errors.
     */
    private final int actionMemory;

    /**
     * Maximum number of TCP connections we're allowed to maintain at any given time due to memory constraints.
     */
    private final int maximumConnections;

    /**
     * When a client detects that it has disconnected from a remote client, it places itself in this queue so
     * that it may be terminated. This allows its resources to be reclaimed.
     *
     * Based on how KryoNet works, I do not think I can have clients close themselves; I'm afraid the program would
     * deadlock. That is, the disconnected() event handler is called by the update thread. Thus, I can't put a call
     * to client.stop() in the disconnected() event handler, as the update() thread would probably deadlock...
     */
    private final BlockingQueue<Client> disconnectedClients;

    /**
     * Use UDP instead of TCP.
     */
    private boolean useUDP;

    /**
     * Constructor.
     *
     * @param conf Configuration passed to the serverless NameNode.
     * @param nameNodeId The unique ID of the local serverless name node instance.
     * @param deploymentNumber The deployment number of the local serverless name node instance.
     * @param serverlessNameNode The ServerlessNameNode instance.
     * @param actionMemory The amount of RAM (in megabytes) that this function has been allocated. Used when
     *                     determining the number of active TCP connections that this NameNode can have at once, as
     *                     each TCP connection has two relatively-large buffers. If the NN creates too many TCP
     *                     connections at once, then it might crash due to OOM errors.
     */
    public NameNodeTcpUdpClient(Configuration conf, ServerlessNameNode serverlessNameNode,
                                long nameNodeId, int deploymentNumber, int actionMemory) {
        this.serverlessNameNode = serverlessNameNode;
        this.nameNodeId = nameNodeId;
        this.deploymentNumber = deploymentNumber;
        this.actionMemory = actionMemory;
        // this.useUDP = conf.getBoolean(DFSConfigKeys.SERVERLESS_USE_UDP, DFSConfigKeys.SERVERLESS_USE_UDP_DEFAULT);

        if (conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING,
                DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING_DEFAULT)) {
            LOG.debug("TCP/UDP Debug logging is ENABLED.");
            com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
        }
        LOG.debug("TCP/UDP Debug logging is DISABLED.");

        this.writeBufferSize = defaultWriteBufferSizeBytes;
        this.objectBufferSize = defaultObjectBufferSizeBytes;

        this.maximumConnections = calculateMaxNumberTcpConnections();

        this.clients = Caffeine.newBuilder()
                .maximumSize(maximumConnections)
                .initialCapacity(maximumConnections)
                // Close TCP clients when they are removed.
                .evictionListener((RemovalListener<ServerlessHopsFSClient, Client>)
                        (serverlessHopsFSClient, client, removalCause) -> {
                    if (client == null)
                        return;

                    LOG.warn("EVICTING CONNECTION: " + serverlessHopsFSClient);

                    // TODO: Per the documentation for RemovalListener, we should not make blocking calls here...
                    if (client.isConnected())
                        client.stop();
                })
                .build();

        this.disconnectedClients = new ArrayBlockingQueue<>(maximumConnections);

        LOG.debug("Created NameNodeTcpUdpClient(NN ID=" + nameNodeId + ", deployment#=" + deploymentNumber +
                ", writeBufferSize=" + writeBufferSize + " bytes, objectBufferSize=" + objectBufferSize +
                " bytes, maximumConnections=" + maximumConnections + ").");
    }

    /**
     * Return the queue of disconnected clients.
     */
    public BlockingQueue<Client> getDisconnectedClients() {
        return disconnectedClients;
    }

    /**
     * Calculate the maximum number of TCP connections that can be maintained at once based on memory constraints.
     *
     * The factors that go into this are the write buffer size, object buffer size, and the amount of memory allocated
     * to the NameNode serverless function. We also need to leave enough RAM for the metadata cache and regular NN
     * operation.
     *
     * I created a function to calculate this rather than in-lining it in the constructor just to keep it contained,
     * as I expect to modify it several times as we fine-tune the formula for determining the number of connections.
     *
     * @return The maximum number of concurrent TCP connections permitted at any given time.
     */
    private int calculateMaxNumberTcpConnections() {
        int combinedBufferSize = maxBufferSize * 2;

        // For now, we reserve 65% of the function's RAM for TCP connection buffers.
        // We multiply by 1e6 to convert to bytes, as the actionMemory variable is in MB.
        int memoryAvailableForConnections = (int) Math.floor(memoryFractionReservedForTcpBuffers * actionMemory * 1000000);

        LOG.debug("There is " + memoryAvailableForConnections + " bytes available for TCP connections.");

        return Math.floorDiv(memoryAvailableForConnections, combinedBufferSize);
    }

    /**
     * Iterate over the TCP connections maintained by this client and check that all of them are still
     * connected. If any are found that are no longer connected, remove them from the list.
     *
     * This is called periodically by the worker thread.
     */
    public synchronized void checkThatClientsAreAllConnected() {
        ArrayList<ServerlessHopsFSClient> toRemove = new ArrayList<>();

        for (Map.Entry<ServerlessHopsFSClient, Client> entry : clients.asMap().entrySet()) {
            ServerlessHopsFSClient serverlessHopsFSClient = entry.getKey();
            Client tcpClient = entry.getValue();

            if (!tcpClient.isConnected()) {
                toRemove.add(serverlessHopsFSClient);
            }
        }

        if (toRemove.size() > 0) {
            LOG.warn("Found " + toRemove.size() + "ServerlessHopsFSClient instance(s) that were disconnected.");

            for (ServerlessHopsFSClient hopsFSClient : toRemove)
                clients.asMap().remove(hopsFSClient);
        }
    }

    /**
     * Register a new Serverless HopsFS client with the NameNode and establish a TCP/UDP connection.
     *
     * This function is thread safe.
     *
     * @param newClient The new Serverless HopsFS client.
     * @return true if the connection was established successfully,
     * false if a connection with that client already exists.
     * @throws IOException If the connection to the new client times out.
     */
    public boolean addClient(ServerlessHopsFSClient newClient) throws IOException {
        useUDP = newClient.getUdpEnabled(); // Set everytime, which is a little annoying.

        if (clients.asMap().containsKey(newClient))
            return false;

        // We pass the writeBuffer and objectBuffer arguments to the Client constructor.
        // Objects are serialized to the write buffer where the bytes are queued until they can
        // be written to the TCP socket. Normally the socket is writable and the bytes are written
        // immediately. If the socket cannot be written to and enough serialized objects are queued
        // to overflow the buffer, then the connection will be closed.
        //
        // One (using only TCP) or three (using both TCP and UDP) buffers are allocated with size
        // equal to the objectBuffer argument (the second parameter). These buffers are used to hold
        // the bytes for a single object graph until it can be sent over the network or deserialized.
        // In short, the object buffers should be sized at least as large as the largest object that will be
        // sent or received.
        Client client = new Client(writeBufferSize, objectBufferSize);
        client.setIdleThreshold(0.25f);

        // Add an entry to the TCP Clients map now so that we do not try to connect again while we're
        // in the process of connecting.
        Client existingClient = clients.asMap().putIfAbsent(newClient, client);
        if (existingClient != null)
            return false;

        client.addListener(new Listener.ThreadedListener(new Listener() {
            /**
             * This listener is responsible for handling messages received from HopsFS clients. These messages will
             * generally be file system operation requests/directions. We will extract the information about the
             * operation, create a task, submit the task to be executed by our worker thread, then return the result
             * back to the HopsFS client.
             * @param connection The connection with the HopsFS client.
             * @param object The object that the client sent to us.
             */
            public void received(Connection connection, Object object) {
                long receivedAtTime = System.currentTimeMillis();
                NameNodeResult result;

                if (object instanceof TcpRequestPayload) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("[TCP/UDP Client] NN " + nameNodeId + " Received work assignment from " +
                                connection.getRemoteAddressTCP() + ".");
                    result = handleWorkAssignment((TcpRequestPayload)object, receivedAtTime);
                }
                else if (object instanceof FrameworkMessage.KeepAlive) {
                    // The server periodically sends KeepAlive objects to prevent the client from disconnecting
                    // due to timeouts. Just ignore these (i.e., do nothing).
                    return; // Make sure to return, or else we'll send an empty message to the client.
                }
                else {
                    LOG.error("[TCP/UDP Client] Received object of unexpected type from client " + connection
                                    + ". Object type: " + object.getClass().getSimpleName() + ": " + object);
                    // Create and log the exception to be returned to the client,
                    // so they know they sent the wrong thing.
                    IllegalArgumentException ex = new IllegalArgumentException(
                            "[TCP/UDP Client] Received object of unexpected type from client " + connection
                                    + ". Object type: " + object.getClass().getSimpleName() + ": " + object);
                    result = new NameNodeResult("N/A", "N/A");
                    result.addException(ex);
                }

                result.prepare(serverlessNameNode.getNamesystem().getMetadataCacheManager());
                sendData(connection, result);
            }

            public void disconnected (Connection connection) {
                clients.invalidate(newClient);           // Remove the client from the cache.

                LOG.warn("[TCP/UDP Client] Disconnected from HopsFS client " + newClient.getClientId() +
                        " at " + newClient.getClientIp() + ":" + newClient.getTcpPort() +
                        ". Active connections: " + clients.estimatedSize() + "/" + maximumConnections);

                // When we detect that we have disconnected, we add ourselves to the queue of disconnected clients.
                // The worker thread will periodically go through and call .stop() on the contents of this queue so
                // that the resources may be reclaimed. See the comment on the 'disconnectedClients' variable as to
                // why we can't just call tcpClient.stop() here. Note that the queue has a bounded size, so this
                // call may block. Normally, we would NOT want to block in these event handlers, but since the client
                // disconnected, we don't care about it. Its update thread can block for all we care; we're just
                // going to dispose of it anyway.
                // disconnectedClients.add(tcpClient);

                client.stop();
            }
        }));

        // We time how long it takes to establish the TCP connection for debugging/metric-collection purposes.
        Instant connectStart = Instant.now();
        Thread connectThread
                = new Thread("T-Conn-" + newClient.getClientIp() + ":" + newClient.getTcpPort() + ":" + newClient.getUdpPort()) {
            public void run() {
                client.start();
                try {
                    // We need to register whatever classes will be serialized BEFORE any network activity is performed.
                    ServerlessClientServerUtilities.registerClassesToBeTransferred(client.getKryo());

                    if (newClient.getUdpEnabled()) {
                        if (LOG.isDebugEnabled()) LOG.debug("Connecting to " + newClient.getClientIp() + ":" + newClient.getTcpPort() + ":" + newClient.getUdpPort() + " via TCP+UDP.");
                        // The call to connect() may produce an IOException if it times out.
                        client.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getTcpPort(), newClient.getUdpPort());
                    } else {
                        if (LOG.isDebugEnabled()) LOG.debug("Connecting to " + newClient.getClientIp() + ":" + newClient.getTcpPort() + " via TCP only.");
                        // The call to connect() may produce an IOException if it times out.
                        client.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getTcpPort());
                    }
                } catch (IOException ex) {
                    LOG.warn("IOException encountered while trying to connect to HopsFS Client via TCP/UDP:", ex);
                }
            }
        };
        connectThread.start();
        try {
            connectThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("InterruptedException encountered while trying to connect to HopsFS Client via TCP/UDP:", ex);
        }
        Instant connectEnd = Instant.now();

        // Compute the duration of the TCP connection establishment.
        Duration connectDuration = Duration.between(connectStart, connectEnd);

        double connectMilliseconds = TimeUnit.NANOSECONDS.toMillis(connectDuration.getNano()) +
                TimeUnit.SECONDS.toMillis(connectDuration.getSeconds());

        if (client.isConnected()) {
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully established connection with client " + newClient.getClientId()
                        + " in " + connectMilliseconds + " milliseconds!");

            client.setKeepAliveTCP(6000);

            // Now that we've registered the classes to be transferred, we can register with the server.
            registerWithClient(client);

            if (LOG.isDebugEnabled()) {
                if (newClient.getUdpEnabled())
                    LOG.debug("[TCP/UDP Client] Successfully added new TCP+UDP client.");
                else
                    LOG.debug("[TCP/UDP Client] Successfully added new TCP-Only client.");
            }

            return true;
        } else {
            // Remove the entry that we added from the TCP client mapping. The connection establishment failed,
            // so we need to remove the record so that we may try again in the future.
            clients.invalidate(newClient);

            if (newClient.getUdpEnabled())
                throw new IOException("Failed to connect to TCP+UDP client at " + newClient.getClientIp() + ":" +
                        newClient.getTcpPort() + ":" + newClient.getUdpEnabled());
            else
                throw new IOException("Failed to connect to TCP-Only client at " + newClient.getClientIp() + ":" +
                        newClient.getTcpPort());
        }
    }

    /**
     * Send an object over a connection via TCP.
     * @param connection The connection over which we're sending an object.
     * @param payload The object to send.
     */
    private void sendData(Connection connection, Object payload) {
        int bytesSent;

        if (useUDP)
            bytesSent = connection.sendUDP(payload);
        else
            bytesSent = connection.sendTCP(payload);

        if (LOG.isDebugEnabled())
            LOG.debug("[TCP/UDP Client] Sent " + bytesSent + " bytes to HopsFS client at " +
                    connection.getRemoteAddressTCP());

        // Increase the buffer size for future TCP connections to hopefully avoid buffer overflows.
        if (bytesSent > objectBufferSize) {
            LOG.warn("[TCP/UDP Client] Sent object of size " + bytesSent
                    + " bytes when object buffer is only of size " + objectBufferSize + " bytes.");

            // Do not go above the max buffer size.
            int oldBufferSize = objectBufferSize;
            objectBufferSize = Math.min(objectBufferSize * 2, maxBufferSize);

            // If we were able to increase the buffer size, then print a message indicating that
            // we did so. If we were already at the max, then we'll print a warning, but currently
            // we don't do anything about it, so future TCP sends of the same object size will fail.
            if (oldBufferSize < objectBufferSize)
                if (LOG.isDebugEnabled()) LOG.debug(
                        "[TCP/UDP Client] Increasing buffer size of future TCP connections to " +
                                objectBufferSize + " bytes.");
            else
                // TODO: What should we do if this occurs?
                LOG.warn("[TCP/UDP Client] Already at the maximum buffer size for TCP connections...");
        }
    }

    /**
     * Execute a file system operation request from a client.
     * @param args The arguments for the function.
     * @param startTime The time at which we received the request.
     * @return The result object that we'll ultimately send back to the client. This contains the result of the
     * FS operation as well as some metric information.
     */
    private NameNodeResult handleWorkAssignment(TcpRequestPayload args, long startTime) {
        String requestId = args.getRequestId();
        BaseHandler.currentRequestId.set(requestId);

        String op = args.getOperationName();
        HashMap<String, Object> fsArgs = args.getFsOperationArguments();
        ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL = args.isConsistencyProtocolEnabled();

        int logLevel = args.getServerlessFunctionLogLevel();
        LogManager.getRootLogger().setLevel(getLogLevelFromInteger(logLevel));

        boolean benchmarkingModeEnabled = args.isBenchmarkingModeEnabled();
        ServerlessNameNode.benchmarkingModeEnabled.set(benchmarkingModeEnabled);
        if (benchmarkingModeEnabled) {
            NameNodeResult tcpResult = new NameNodeResult(requestId, op);
            serverlessNameNode.getExecutionManager().tryExecuteTask(
                    requestId, op, new HashMapTaskArguments(fsArgs), false, tcpResult, false);
            return tcpResult;
        } else {
            NameNodeResultWithMetrics tcpResult = new NameNodeResultWithMetrics(deploymentNumber, requestId,
                    "TCP", serverlessNameNode.getId(), op);
            tcpResult.setFnStartTime(startTime);
            serverlessNameNode.getExecutionManager().tryExecuteTask(
                    requestId, op, new HashMapTaskArguments(fsArgs), false, tcpResult, false);
            return tcpResult;
        }
    }

//    private NameNodeResult handleWorkAssignment(JsonObject args, long startTime) {
//        String requestId = args.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_ID).getAsString();
//        String op = args.getAsJsonPrimitive(ServerlessNameNodeKeys.OPERATION).getAsString();
//        JsonObject fsArgs = args.getAsJsonObject(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS);
//
//        if (args.has(LOG_LEVEL)) {
//            String logLevel = args.get(LOG_LEVEL).getAsString();
//            LogManager.getRootLogger().setLevel(getLogLevelFromString(logLevel));
//        }
//
//        if (args.has(CONSISTENCY_PROTOCOL_ENABLED)) {
//            ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL = args.get(CONSISTENCY_PROTOCOL_ENABLED).getAsBoolean();
//        }
//
//        NameNodeResult tcpResult = new NameNodeResult(deploymentNumber, requestId, "TCP", serverlessNameNode.getId(), op);
//        tcpResult.setFnStartTime(startTime);
//
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Received TCP Message. RequestID=" + requestId + ", OpName: " + op);
//        }
//
//        // Create a new task. After this, we assign it to the worker thread and wait for the
//        // result to be computed before returning it to the user.
//        // FileSystemTask<Serializable> task = new FileSystemTask<>(requestId, op, fsArgs, false, "TCP");
//
//        BaseHandler.currentRequestId.set(requestId);
//
//        //serverlessNameNode.getExecutionManager().tryExecuteTask(task, tcpResult, false);
//        serverlessNameNode.getExecutionManager().tryExecuteTask(
//                requestId, op, fsArgs, false, tcpResult, false);
//        return tcpResult;
//    }

    /**
     * Complete the registration phase with the client's TCP server.
     * @param client The TCP connection established with the client.
     */
    private void registerWithClient(Client client) {
        // Complete the registration with the TCP server.
        JsonObject registration = new JsonObject();
        registration.addProperty(ServerlessNameNodeKeys.OPERATION, ServerlessClientServerUtilities.OPERATION_REGISTER);
        registration.addProperty(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER, deploymentNumber);
        registration.addProperty(ServerlessNameNodeKeys.NAME_NODE_ID, nameNodeId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending registration to HopsFS client @ " + client.getRemoteAddressTCP() + " via " +
                    (useUDP ? "UDP" : "TCP") + " now...");
        }

        int bytesSent;
        if (useUDP) {
            bytesSent = client.sendUDP(registration.toString());
        } else {
            bytesSent = client.sendTCP(registration.toString());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sent " + bytesSent + " bytes for registration to HopsFS client @ " +
                    client.getRemoteAddressTCP() + " via " + (useUDP ? "UDP" : "TCP") + " now...");
        }
    }

    /**
     * Unregister the given HopsFS client with the NameNode. Terminates the TCP connection to this client.
     * @param client The HopsFS client to unregister.
     * @return true if unregistered successfully, false if the client was already not registered with the NameNode.
     */
    public boolean removeClient(ServerlessHopsFSClient client) {
        Client tcpClient = clients.getIfPresent(client);

        if (tcpClient == null) {
            LOG.warn("No TCP client associated with HopsFS client " + client.toString());
            return false;
        }

        // Stop the TCP client. This function calls the close() method.
        tcpClient.stop();

        clients.invalidate(client);

        return true;
    }

    /**
     * @return The number of active TCP clients connected to the NameNode.
     */
    public int numClients() {
        return (int) clients.estimatedSize();
    }
}
