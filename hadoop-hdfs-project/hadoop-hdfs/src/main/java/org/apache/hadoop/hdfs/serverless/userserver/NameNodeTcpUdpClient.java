package org.apache.hadoop.hdfs.serverless.userserver;

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
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.consistency.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.execution.taskarguments.HashMapTaskArguments;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResultWithMetrics;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
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
    //private final Cache<ServerlessHopsFSClient, Client> clients;
    private final Cache<String, Client> clients;

    /**
     * Number of connections per VM.
     */
    private final ConcurrentHashMap<String, Set<Integer>> connectionsPerVm;

    /**
     * The fraction of main memory reserved for TCP/UDP connection buffers.
     */
    private static final float memoryFractionReservedForTcpBuffers = 0.11f;

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
     * The size, in bytes, used for the "Write Buffer" of new TCP connections. Objects are serialized to
     * the "Write Buffer" where the bytes are queued until they can be written to the TCP socket.
     */
    private static final int defaultWriteBufferSizeBytes = (int)1e6;

    /**
     * The size, in bytes, used for the object buffer of new TCP connections. Object buffers are used
     * to hold the bytes for a single object graph until it can be sent over the network or deserialized.
     */
    private static final int defaultObjectBufferSizeBytes = (int)1e6;

    /**
     * The maximum size, in bytes, that can be used for a TCP write buffer or a TCP object buffer.
     *
     * If we find that we're trying to write data that is larger than the buffer(s), then we change the
     * size of the buffers for future TCP connections to hopefully avoid the problem. This variable sets a hard limit
     * on the maximum size of a buffer.
     */
    private static final int maxBufferSize =(int)1e6;

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
     * TODO: Currently unused. I actually call client.stop() in the disconnected handler... Is this bad?
     *
     * When a client detects that it has disconnected from a remote client, it places itself in this queue so
     * that it may be terminated. This allows its resources to be reclaimed.
     *
     * Based on how KryoNet works, I do not think I can have clients close themselves; I'm afraid the program would
     * deadlock. That is, the disconnected() event handler is called by the update thread. Thus, I can't put a call
     * to client.stop() in the disconnected() event handler, as the update() thread would probably deadlock...
     */
    // private final BlockingQueue<Client> disconnectedClients;

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

        if (conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING,
                DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING_DEFAULT)) {
            LOG.debug("TCP/UDP Debug logging is ENABLED.");
            com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
        }
        LOG.debug("TCP/UDP Debug logging is DISABLED.");

        this.writeBufferSize = defaultWriteBufferSizeBytes;
        this.objectBufferSize = defaultObjectBufferSizeBytes;

        this.maximumConnections = calculateMaxNumberTcpConnections();

        this.connectionsPerVm = new ConcurrentHashMap<>();

        this.clients = Caffeine.newBuilder()
                .maximumSize(maximumConnections)
                .initialCapacity(maximumConnections)
                // Close TCP clients when they are removed.
                .evictionListener((RemovalListener<String, Client>)
                        (clientIp, client, removalCause) -> {
                    if (client == null)
                        return;

                    LOG.warn("EVICTING CONNECTION: " + clientIp);

                    // TODO: Per the documentation for RemovalListener, we should not make blocking calls here...
                    if (client.isConnected())
                        client.stop();
                })
                .build();

        LOG.info("Created NameNodeTcpUdpClient(NN ID=" + nameNodeId + ", deployment#=" + deploymentNumber +
                ", writeBufferSize=" + writeBufferSize + " bytes, objectBufferSize=" + objectBufferSize +
                " bytes, maximumConnections=" + maximumConnections + ").");
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

        // We multiply by 1e6 to convert to bytes, as the actionMemory variable is in MB.
        int memoryAvailableForConnections = (int) Math.floor(memoryFractionReservedForTcpBuffers * actionMemory * 1.0e6);

        LOG.debug("There are " + memoryAvailableForConnections + " bytes available for TCP connections.");

        return Math.floorDiv(memoryAvailableForConnections, combinedBufferSize);
    }

    public synchronized boolean connectionExists(ServerlessHopsFSClient client) {
        return clients.asMap().containsKey(client.getTcpString());
    }

    public synchronized boolean connectionExists(String host, int tcpPort) {
        Set<Integer> connectedPorts = connectionsPerVm.get(host);

        if (connectedPorts == null) return false;

        return connectedPorts.contains(tcpPort);
        // return clients.asMap().containsKey(host + ":" + tcpPort);
    }

    /**
     * Atomically check if a connection exists to the HopsFS client represented by the provided
     * {@link ServerlessHopsFSClient} instance.
     *
     * If no connection exists, we create a new {@link Client} instance, add it to the internal client mapping,
     * and return the TCP client instance.
     *
     * If a connection DOES exist, then we just return null.
     *
     * @param client The {@link ServerlessHopsFSClient} representing a HopsFS client whom we should create a TCP
     *               connection to if one does not already exist.
     *
     * @return The {@link Client} instance created for the {@link ServerlessHopsFSClient} if a connection does not
     * already exist. Otherwise, (if a connection already exists) return null.
     */
    private synchronized Client addClientIfNew(ServerlessHopsFSClient client) {
        // First, check if a connection exists. If it does, then return null.
        if (connectionExists(client))
            return null;

        Client tcpClient = new Client(writeBufferSize, objectBufferSize);
        tcpClient.setIdleThreshold(0.25f);

        Client existingClient = clients.asMap().putIfAbsent(
                client.getTcpString(), tcpClient);
        if (existingClient != null)
            return null;

        return tcpClient;
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

        // COMMENTED OUT:
        // We just use the `addClientIfNew` function now.

        // if (connectionExists(newClient))
        //     return false;

        // We pass the writeBuffer and objectBuffer arguments to the Client constructor.
        // Objects are serialized to the "Write Buffer" where the bytes are queued until they can
        // be written to the TCP socket. Normally the socket is writable and the bytes are written
        // immediately. If the socket cannot be written to and enough serialized objects are queued
        // to overflow the buffer, then the connection will be closed.
        //
        // One (using only TCP) or three (using both TCP and UDP) buffers are allocated with size
        // equal to the objectBuffer argument (the second parameter). These buffers are used to hold
        // the bytes for a single object graph until it can be sent over the network or deserialized.
        // In short, the object buffers should be sized at least as large as the largest object that will be
        // sent or received.
        //  tcpClient = new Client(writeBufferSize, objectBufferSize);
        // tcpClient.setIdleThreshold(0.25f);

        // Add an entry to the TCP Clients map now so that we do not try to connect again while we're
        // in the process of connecting.
        // Client existingClient = clients.asMap().putIfAbsent(newClient, tcpClient);
        // if (existingClient != null)
        //    return false;

        Client tcpClient = addClientIfNew(newClient);
        if (tcpClient == null) {
            if (LOG.isDebugEnabled())
                LOG.debug("Already have connection with client " + newClient + ". Aborting connection establishment.");

            return false;
        } else {
            if (LOG.isDebugEnabled())
                LOG.debug("Attempting to connect to new client connection " + newClient);
        }

        tcpClient.addListener(new Listener.ThreadedListener(new Listener() {
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

                if (object instanceof TcpUdpRequestPayload) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("[TCP/UDP Client] NN " + nameNodeId + " Received work from " +
                                connection.getRemoteAddressTCP() + ".");
                    result = handleWorkAssignment((TcpUdpRequestPayload)object, receivedAtTime, newClient);
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
                // Remove the client from the cache.
                clients.invalidate(newClient.getTcpString());

                LOG.warn("[TCP/UDP Client] Disconnected from HopsFS client " + newClient.getClientId() +
                        " at " + newClient.getTcpString() + ". Active connections: " + clients.estimatedSize() +
                        "/" + maximumConnections);

                // When we detect that we have disconnected, we add ourselves to the queue of disconnected clients.
                // The worker thread will periodically go through and call .stop() on the contents of this queue so
                // that the resources may be reclaimed. See the comment on the 'disconnectedClients' variable as to
                // why we can't just call tcpClient.stop() here. Note that the queue has a bounded size, so this
                // call may block. Normally, we would NOT want to block in these event handlers, but since the client
                // disconnected, we don't care about it. Its update thread can block for all we care; we're just
                // going to dispose of it anyway.
                // disconnectedClients.add(tcpClient);
                Set<Integer> connectedPorts = connectionsPerVm.computeIfAbsent(
                        newClient.getClientIp(), ip -> ConcurrentHashMap.newKeySet());
                connectedPorts.remove(newClient.getTcpPort());

                tcpClient.stop();
            }
        }));

        // We time how long it takes to establish the TCP connection for debugging/metric-collection purposes.
        Instant connectStart = Instant.now();
        Thread connectThread
                = new Thread("T-Conn-" + newClient.getTcpString() + ":" + newClient.getUdpPort()) {
            public void run() {
                tcpClient.start();
                try {
                    // We need to register whatever classes will be serialized BEFORE any network activity is performed.
                    ServerlessClientServerUtilities.registerClassesToBeTransferred(tcpClient.getKryo());

                    if (newClient.getUdpEnabled()) {
                        if (LOG.isDebugEnabled()) LOG.debug("Connecting to " + newClient.getTcpString() + ":" + newClient.getUdpPort() + " via TCP+UDP.");
                        // The call to connect() may produce an IOException if it times out.
                        tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getTcpPort(), newClient.getUdpPort());
                    } else {
                        if (LOG.isDebugEnabled()) LOG.debug("Connecting to " + newClient.getTcpString() + " via TCP only.");
                        // The call to connect() may produce an IOException if it times out.
                        tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getTcpPort());
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

        if (tcpClient.isConnected()) {
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully established connection with client " + newClient.getClientId()
                        + " in " + connectMilliseconds + " milliseconds! There are now approximately " +
                        clients.estimatedSize() + " active connections.");

            tcpClient.setKeepAliveTCP(5000);
            tcpClient.setTimeout(20000);

            // Now that we've registered the classes to be transferred, we can register with the server.
            registerWithClient(tcpClient);

            if (LOG.isDebugEnabled()) {
                if (newClient.getUdpEnabled())
                    LOG.debug("[TCP/UDP Client] Successfully added new TCP+UDP client.");
                else
                    LOG.debug("[TCP/UDP Client] Successfully added new TCP-Only client.");
            }

            connectionsPerVm.compute(newClient.getClientIp(), (ip, connectedPorts) -> {
               if (connectedPorts == null)
                   connectedPorts = ConcurrentHashMap.newKeySet();

               connectedPorts.add(newClient.getTcpPort());
               return connectedPorts;
            });

            return true;
        } else {
            // Remove the entry that we added from the TCP client mapping. The connection establishment failed,
            // so we need to remove the record so that we may try again in the future.
            clients.invalidate(newClient.getTcpString());

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

        // TODO: We may want to serialize the object BEFORE calling connection.sendTCP().
        //       Then, we can check if the bytes are larger than the buffer size. If they
        //       are, then we can send the object in pieces, and it can be reconstructed
        //       at the client. This reconstruction can occur within the TCP server. That is,
        //       the TCP server can determine if it has received a chunk of an object or the
        //       full payload. If it received a chunk, then it waits until all chunks are
        //       received before handing it off to whoever is waiting on it.
        // TODO: (continued) This issue is that we want to use small buffer sizes, as many operations,
        //       such as file reads/writes, require a very small amount of data to be sent. But operations
        //       such as listing the contents of a directory with a large number of files will require
        //       more (e.g., a directory with ~1000 files requires just over 60,000 bytes).

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
    private NameNodeResult handleWorkAssignment(TcpUdpRequestPayload args, long startTime, ServerlessHopsFSClient newClient) {
        String requestId = args.getRequestId();
        BaseHandler.currentRequestId.set(requestId);

        String op = args.getOperationName();
        HashMap<String, Object> fsArgs = args.getFsOperationArguments();
        ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL = args.isConsistencyProtocolEnabled();

        int logLevel = args.getServerlessFunctionLogLevel();
        LogManager.getRootLogger().setLevel(getLogLevelFromInteger(logLevel));
        //LogManager.getRootLogger().setLevel(Level.TRACE);

        boolean benchmarkingModeEnabled = args.isBenchmarkingModeEnabled();
        ServerlessNameNode.benchmarkingModeEnabled.set(benchmarkingModeEnabled);
        if (benchmarkingModeEnabled) {
            NameNodeResult tcpResult = new NameNodeResult(requestId, op);
            serverlessNameNode.getExecutionManager().tryExecuteTask(
                    requestId, op, new HashMapTaskArguments(fsArgs), false, tcpResult, false);

            long s = System.nanoTime();
            // Only bother trying to connect if there's at least one non-existent connection.
            if (connectionsPerVm.get(newClient.getClientIp()).size() != args.getActiveTcpPorts().size()) {
                // TODO: Determine how this impacts performance.
                OpenWhiskHandler.attemptToConnectToClient(serverlessNameNode,
                        args.getActiveTcpPorts(), args.getActiveUdpPorts(), newClient.getClientIp(),
                        newClient.getClientId(), useUDP);
            }
            if (LOG.isDebugEnabled()) {
                long t = System.nanoTime();
                LOG.debug("Attempted additional connections in " + ((t - s) / 1.0e6) + " ms.");
            }

            return tcpResult;
        } else {
            NameNodeResultWithMetrics tcpResult = new NameNodeResultWithMetrics(deploymentNumber, requestId,
                    "TCP", serverlessNameNode.getId(), op);
            tcpResult.setFnStartTime(startTime);
            serverlessNameNode.getExecutionManager().tryExecuteTask(
                    requestId, op, new HashMapTaskArguments(fsArgs), false, tcpResult, false);

            long s = System.nanoTime();
            // Only bother trying to connect if there's at least one non-existent connection.
            if (connectionsPerVm.get(newClient.getClientIp()).size() != args.getActiveTcpPorts().size()) {
                // TODO: Determine how this impacts performance.
                OpenWhiskHandler.attemptToConnectToClient(serverlessNameNode,
                        args.getActiveTcpPorts(), args.getActiveUdpPorts(), newClient.getClientIp(),
                        newClient.getClientId(), useUDP);
            }
            if (LOG.isDebugEnabled()) {
                long t = System.nanoTime();
                LOG.debug("Attempted additional connections in " + ((t - s) / 1.0e6) + " ms.");
            }

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
        Client tcpClient = clients.getIfPresent(client.getTcpString());

        if (tcpClient == null) {
            LOG.warn("No TCP client associated with HopsFS client " + client.toString());
            return false;
        }

        // Stop the TCP client. This function calls the close() method.
        tcpClient.stop();

        clients.invalidate(client.getTcpString());

        return true;
    }

    /**
     * @return The number of active TCP clients connected to the NameNode.
     */
    public int numClients() {
        return (int) clients.estimatedSize();
    }
}
