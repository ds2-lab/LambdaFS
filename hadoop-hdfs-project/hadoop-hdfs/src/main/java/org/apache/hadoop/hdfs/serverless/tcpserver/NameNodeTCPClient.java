package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.util.TcpIdleSender;
import com.github.benmanes.caffeine.cache.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.BaseHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.operation.execution.NameNodeResult;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.serverless.OpenWhiskHandler.getLogLevelFromString;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.CONSISTENCY_PROTOCOL_ENABLED;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.LOG_LEVEL;

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
public class NameNodeTCPClient {
    private static final Log LOG = LogFactory.getLog(NameNodeTCPClient.class);

    /**
     * This is the maximum amount of time a call to connect() will block. Calls to connect() occur when
     * establishing a connection to a new client.
     */
    private static final int CONNECTION_TIMEOUT = 8000;

    /**
     * Mapping from instances of ServerlessHopsFSClient to their associated TCP client object. Recall that each
     * ServerlessHopsFSClient represents a particular client of HopsFS that the NameNode is talking to. We map
     * each of these "HopsFS client" representations to the TCP Client associated with them.
     */
    private final Cache<ServerlessHopsFSClient, Client> tcpClients;

    /**
     * The fraction of main memory reserved for TCP connection buffers.
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
    public NameNodeTCPClient(Configuration conf, ServerlessNameNode serverlessNameNode,
                             long nameNodeId, int deploymentNumber, int actionMemory) {
        this.serverlessNameNode = serverlessNameNode;
        this.nameNodeId = nameNodeId;
        this.deploymentNumber = deploymentNumber;
        this.actionMemory = actionMemory;

        if (conf.getBoolean(DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING,
                DFSConfigKeys.SERVERLESS_TCP_DEBUG_LOGGING_DEFAULT)) {
            LOG.debug("TCP Debug logging is ENABLED.");
            com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
        }
        LOG.debug("TCP Debug logging is DISABLED.");

        this.writeBufferSize = defaultWriteBufferSizeBytes;
        this.objectBufferSize = defaultObjectBufferSizeBytes;

        this.maximumConnections = calculateMaxNumberTcpConnections();

        this.tcpClients = Caffeine.newBuilder()
                .maximumSize(maximumConnections)
                .initialCapacity(maximumConnections)
                // Close TCP clients when they are removed.
                .evictionListener((RemovalListener<ServerlessHopsFSClient, Client>) (serverlessHopsFSClient, client, removalCause) -> {
                    if (client == null)
                        return;

                    LOG.warn("EVICTING CONNECTION: " + serverlessHopsFSClient);

                    // TODO: Per the documentation for RemovalListener, we should not make blocking calls here...
                    if (client.isConnected())
                        client.stop();
                })
                .build();

        this.disconnectedClients = new ArrayBlockingQueue<>(maximumConnections);

        LOG.debug("Created NameNodeTCPClient(NN ID=" + nameNodeId + ", deployment#=" + deploymentNumber +
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

        for (Map.Entry<ServerlessHopsFSClient, Client> entry : tcpClients.asMap().entrySet()) {
            ServerlessHopsFSClient serverlessHopsFSClient = entry.getKey();
            Client tcpClient = entry.getValue();

            if (!tcpClient.isConnected()) {
                toRemove.add(serverlessHopsFSClient);
            }
        }

        if (toRemove.size() > 0) {
            LOG.warn("Found " + toRemove.size() + "ServerlessHopsFSClient instance(s) that were disconnected.");

            for (ServerlessHopsFSClient hopsFSClient : toRemove)
                tcpClients.asMap().remove(hopsFSClient);
        }
    }

    /**
     * Register a new Serverless HopsFS client with the NameNode and establish a TCP connection with the new client.
     *
     * This function is thread safe.
     *
     * @param newClient The new Serverless HopsFS client.
     * @return true if the connection was established successfully,
     * false if a connection with that client already exists.
     * @throws IOException If the connection to the new client times out.
     */
    public boolean addClient(ServerlessHopsFSClient newClient) throws IOException {
        if (tcpClients.asMap().containsKey(newClient)) {
            LOG.warn("NameNodeTCPClient already has a connection to client " + newClient);
            return false;
        }

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
        Client tcpClient = new Client(writeBufferSize, objectBufferSize);
        tcpClient.setIdleThreshold(0.25f);

        // Add an entry to the TCP Clients map now so that we do not try to connect again while we're
        // in the process of connecting.
        Client existingClient = tcpClients.asMap().putIfAbsent(newClient, tcpClient);
        if (existingClient != null) {
            LOG.warn("Actually, NameNodeTCPClient already has a connection to client " + newClient);
            return false;
        }

//        Kryo clientWriteKryo = new Kryo();
//        ServerlessClientServerUtilities.registerClassesToBeTransferred(clientWriteKryo);

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
                NameNodeResult tcpResult;
                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof String) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("[TCP Client] NN " + nameNodeId + " Received work assignment from " + connection.getRemoteAddressTCP() + ".");
//                        LOG.debug("[TCP Client] NN " + nameNodeId + " Received work assignment from " +
//                                        connection.getRemoteAddressTCP() + ". current heap size: " +
//                                        (Runtime.getRuntime().totalMemory() / 1000000.0) +  " MB, free space in heap: " +
//                                        (Runtime.getRuntime().freeMemory() / 1000000.0) + " MB.");
                    JsonObject jsonObject = new JsonParser().parse((String)object).getAsJsonObject();
                    tcpResult = handleWorkAssignment(jsonObject, receivedAtTime);
                }
                else if (object instanceof FrameworkMessage.KeepAlive) {
                    // The server periodically sends KeepAlive objects to prevent the client from disconnecting
                    // due to timeouts. Just ignore these (i.e., do nothing).
                    return; // Make sure to return, or else we'll send an empty message to the client.
                }
                else {
                    // Create and log the exception to be returned to the client,
                    // so they know they sent the wrong thing.
                    IllegalArgumentException ex = new IllegalArgumentException(
                            "[TCP Client] Received object of unexpected type from client " + connection
                                    + ". Object type: " + object.getClass().getSimpleName() + ".");
                    tcpResult = new NameNodeResult(deploymentNumber, "N/A", "TCP",
                            serverlessNameNode.getId(), "N/A");
                    tcpResult.addException(ex);
                }

                long s = System.currentTimeMillis();
//                String jsonString = new Gson().toJson(tcpResult.toJson(ServerlessClientServerUtilities.OPERATION_RESULT,
//                        serverlessNameNode.getNamesystem().getMetadataCacheManager()));

//                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
//                String jsonString = null;
//                try {
//                    jsonString = ow.writeValueAsString(tcpResult.toJsonJackson(
//                            ServerlessClientServerUtilities.OPERATION_RESULT,
//                            serverlessNameNode.getNamesystem().getMetadataCacheManager()));
//                } catch (JsonProcessingException e) {
//                    e.printStackTrace();
//                }
//
//                long t = System.currentTimeMillis();
//
//                if (t - s > 10)
//                    LOG.warn("Converting NameNodeResult instance to JSON took " + (t - s) + " ms.");

//                Kryo kryo = tcpClient.getKryo();
//
//                LOG.debug("Trying to write the NameNodeResult object for debugging purposes now.");
//                long start = System.currentTimeMillis();
//                Output output = new Output(32768, -1);
//                kryo.writeObject(output, tcpResult);
//                long end = System.currentTimeMillis();
//                LOG.debug("Wrote NameNodeResult object to Output in " + (end - start) + " ms.");

//                Input input = new Input(output.getBuffer(), 0, output.position());
//                NameNodeResult tcpResult2 = kryo.readObject(input, NameNodeResult.class);
//                LOG.debug("Read TCP result back in " + (System.currentTimeMillis() - end) +
//                        " ms. TcpResult: " + tcpResult2);

                trySendTcp(connection, tcpResult);
            }

            public void disconnected (Connection connection) {
                tcpClients.invalidate(newClient);           // Remove the client from the cache.

                LOG.warn("[TCP Client] Disconnected from HopsFS client " + newClient.getClientId() +
                        " at " + newClient.getClientIp() + ":" + newClient.getClientPort() +
                        ". Active TCP connections: " + tcpClients.estimatedSize() + "/" + maximumConnections);

                // When we detect that we have disconnected, we add ourselves to the queue of disconnected clients.
                // The worker thread will periodically go through and call .stop() on the contents of this queue so
                // that the resources may be reclaimed. See the comment on the 'disconnectedClients' variable as to
                // why we can't just call tcpClient.stop() here. Note that the queue has a bounded size, so this
                // call may block. Normally, we would NOT want to block in these event handlers, but since the client
                // disconnected, we don't care about it. Its update thread can block for all we care; we're just
                // going to dispose of it anyway.
                // disconnectedClients.add(tcpClient);

                tcpClient.stop();
            }
        }));

        // We time how long it takes to establish the TCP connection for debugging/metric-collection purposes.
        Instant connectStart = Instant.now();
        Thread connectThread
                = new Thread("Thread-ConnectTo" + newClient.getClientIp() + ":" + newClient.getClientPort()) {
            public void run() {
                tcpClient.start();
                try {
                    // We need to register whatever classes will be serialized BEFORE any network activity is performed.
                    ServerlessClientServerUtilities.registerClassesToBeTransferred(tcpClient.getKryo());

                    // The call to connect() may produce an IOException if it times out.
                    tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getClientPort());
                } catch (IOException ex) {
                    LOG.warn("IOException encountered while trying to connect to HopsFS Client via TCP:", ex);
                }
            }
        };
        connectThread.start();
        try {
            connectThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("InterruptedException encountered while trying to connect to HopsFS Client via TCP:", ex);
        }
        Instant connectEnd = Instant.now();

        // Compute the duration of the TCP connection establishment.
        Duration connectDuration = Duration.between(connectStart, connectEnd);

        double connectMilliseconds = TimeUnit.NANOSECONDS.toMillis(connectDuration.getNano()) +
                TimeUnit.SECONDS.toMillis(connectDuration.getSeconds());

        if (tcpClient.isConnected()) {
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully established connection with client " + newClient.getClientId()
                        + " in " + connectMilliseconds + " milliseconds!");

            tcpClient.setKeepAliveTCP(6000);

            // Now that we've registered the classes to be transferred, we can register with the server.
            registerWithClient(tcpClient);

            if (LOG.isDebugEnabled())
                LOG.debug("[TCP Client] Successfully added new TCP client.");

            return true;
        } else {
            // Remove the entry that we added from the TCP client mapping. The connection establishment failed,
            // so we need to remove the record so that we may try again in the future.
            tcpClients.invalidate(newClient);
            throw new IOException("Failed to connect to client at " + newClient.getClientIp() + ":" +
                    newClient.getClientPort());
        }
    }

    /**
     * Try to send an object over a connection via TCP. This will fail of the TCP buffer is 90% full or more.
     * If this fails, then it enqueues the object to be sent when the buffer is not as full.
     *
     * @param connection The connection over which we're sending an object.
     * @param payload The object to send.
     */
    private void trySendTcp(Connection connection, NameNodeResult payload) {
        double currentCapacity = ((double) connection.getTcpWriteBufferSize()) / ((double) writeBufferSize);
        if (currentCapacity >= 0.9) {
            LOG.warn("[TCP Client] Write buffer for connection " + connection.getRemoteAddressTCP() +
                    " is at " + (currentCapacity * 100) + "% capacity! Enqueuing payload to send later...");
            connection.addListener(new TcpIdleSender() {
                boolean _started = false;
                NameNodeResult enqueuedObject = payload;

                @Override
                protected NameNodeResult next() {
                    if (LOG.isDebugEnabled())
                        LOG.debug("[TCP Client] Write buffer for connection " +  connection.getRemoteAddressTCP() +
                                " has reached 'idle' capacity. Sending buffered object now.");
                    NameNodeResult toReturn = enqueuedObject;

                    // Set this to null before we return so that we cannot get stuck in a loop of returning this
                    // object from this listener. This is just a safeguard; that loop scenario should never occur.
                    enqueuedObject = null;
                    return toReturn;
                }

                @Override
                public void idle(Connection connection) {
                    if (!_started) {    // This part is just from the original idle() method that I'm overloading.
                                        // I'm really not sure what it does or why it exists, as the start()
                                        // function doesn't actually do anything...
                        _started = true;
                        start();
                    }
                    // So, first we remove this listener from the connection so that it never activates again.
                    connection.removeListener(this);

                    // Next, retrieve the enqueued object to send to the client.
                    NameNodeResult object = next();

                    // If the enqueued object is NOT null, then we will try to send it. This is sort of a recursive
                    // call, since this listener was created within the trySendTcp() function. But we've already
                    // removed this listener from the connection, so we'll effectively go away once this idle()
                    // function exits. We just don't want to send the object without making sure the buffer is
                    // still clear. So, we try again. If it fails again, then at least this object gets enqueued,
                    // so it should eventually make it to the client.
                    if (object != null) {
                        trySendTcp(connection, object);
                    }
                }
            });
        }
        else {
//            if (LOG.isDebugEnabled())
//                LOG.debug("[TCP Client] Write buffer for connection " + connection.getRemoteAddressTCP() +
//                        " is at " + (currentCapacity * 100) + "% capacity! Sending payload immediately.");
            payload.prepare(serverlessNameNode.getNamesystem().getMetadataCacheManager());
            sendTcp(connection, payload);
        }
    }

    /**
     * Send an object over a connection via TCP.
     * @param connection The connection over which we're sending an object.
     * @param payload The object to send.
     */
    private void sendTcp(Connection connection, Object payload) {
        int bytesSent = connection.sendTCP(payload);

        if (LOG.isDebugEnabled())
            LOG.debug("[TCP Client] Sent " + bytesSent + " bytes to HopsFS client at " +
                    connection.getRemoteAddressTCP());

        // Increase the buffer size for future TCP connections to hopefully avoid buffer overflows.
        if (bytesSent > objectBufferSize) {
            LOG.warn("[TCP Client] Sent object of size " + bytesSent
                    + " bytes when object buffer is only of size " + objectBufferSize + " bytes.");

            // Do not go above the max buffer size.
            int oldBufferSize = objectBufferSize;
            objectBufferSize = Math.min(objectBufferSize * 2, maxBufferSize);

            // If we were able to increase the buffer size, then print a message indicating that
            // we did so. If we were already at the max, then we'll print a warning, but currently
            // we don't do anything about it, so future TCP sends of the same object size will fail.
            if (oldBufferSize < objectBufferSize)
                if (LOG.isDebugEnabled()) LOG.debug("[TCP Client] Increasing buffer size of future TCP connections to " + objectBufferSize + " bytes.");
            else
                // TODO: What should we do if this occurs?
                LOG.warn("[TCP Client] Already at the maximum buffer size for TCP connections...");
        }
    }

    private NameNodeResult handleWorkAssignment(JsonObject args, long startTime) {
        String requestId = args.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_ID).getAsString();
        String op = args.getAsJsonPrimitive(ServerlessNameNodeKeys.OPERATION).getAsString();
        JsonObject fsArgs = args.getAsJsonObject(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS);

        if (args.has(LOG_LEVEL)) {
            String logLevel = args.get(LOG_LEVEL).getAsString();
            LogManager.getRootLogger().setLevel(getLogLevelFromString(logLevel));
        }

        if (args.has(CONSISTENCY_PROTOCOL_ENABLED)) {
            ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL = args.get(CONSISTENCY_PROTOCOL_ENABLED).getAsBoolean();
        }

        NameNodeResult tcpResult = new NameNodeResult(deploymentNumber, requestId, "TCP",
                serverlessNameNode.getId(), op);
        tcpResult.setFnStartTime(startTime);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received TCP Message. RequestID=" + requestId + ", OpName: " + op);
        }

        // Create a new task. After this, we assign it to the worker thread and wait for the
        // result to be computed before returning it to the user.
        // FileSystemTask<Serializable> task = new FileSystemTask<>(requestId, op, fsArgs, false, "TCP");

        BaseHandler.currentRequestId.set(requestId);

        //serverlessNameNode.getExecutionManager().tryExecuteTask(task, tcpResult, false);
        serverlessNameNode.getExecutionManager().tryExecuteTask(
                requestId, op, fsArgs, false, tcpResult, false);
        return tcpResult;
    }

    /**
     * Complete the registration phase with the client's TCP server.
     * @param tcpClient The TCP connection established with the client.
     */
    private void registerWithClient(Client tcpClient) {
        // Complete the registration with the TCP server.
        JsonObject registration = new JsonObject();
        registration.addProperty(ServerlessNameNodeKeys.OPERATION, ServerlessClientServerUtilities.OPERATION_REGISTER);
        registration.addProperty(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER, deploymentNumber);
        registration.addProperty(ServerlessNameNodeKeys.NAME_NODE_ID, nameNodeId);

        if (LOG.isDebugEnabled())
            LOG.debug("Registering with HopsFS client at " + tcpClient.getRemoteAddressTCP() + " now...");
        int bytesSent = tcpClient.sendTCP(registration.toString());
        //LOG.debug("Sent " + bytesSent + " bytes to HopsFS client at " +  tcpClient.getRemoteAddressTCP() +
        //        " during registration.");
    }

    /**
     * Unregister the given HopsFS client with the NameNode. Terminates the TCP connection to this client.
     * @param client The HopsFS client to unregister.
     * @return true if unregistered successfully, false if the client was already not registered with the NameNode.
     */
    public boolean removeClient(ServerlessHopsFSClient client) {
        Client tcpClient = tcpClients.getIfPresent(client);

        if (tcpClient == null) {
            LOG.warn("No TCP client associated with HopsFS client " + client.toString());
            return false;
        }

        // Stop the TCP client. This function calls the close() method.
        tcpClient.stop();

        tcpClients.invalidate(client);

        return true;
    }

    /**
     * @return The number of active TCP clients connected to the NameNode.
     */
    public int numClients() {
        return (int)tcpClients.estimatedSize();
    }
}
