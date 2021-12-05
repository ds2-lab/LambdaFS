package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.github.benmanes.caffeine.cache.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.FileSystemTaskUtils;
import org.apache.hadoop.hdfs.serverless.operation.NameNodeResult;
import org.apache.hadoop.util.Time;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

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
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(NameNodeTCPClient.class);

    /**
     * This is the maximum amount of time a call to connect() will block. Calls to connect() occur when
     * establishing a connection to a new client.
     */
    private static final int CONNECTION_TIMEOUT = 5000;

    /**
     * Mapping from instances of ServerlessHopsFSClient to their associated TCP client object. Recall that each
     * ServerlessHopsFSClient represents a particular client of HopsFS that the NameNode is talking to. We map
     * each of these "HopsFS client" representations to the TCP Client associated with them.
     */
    private final Cache<ServerlessHopsFSClient, Client> tcpClients;

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
    private static final int defaultWriteBufferSizeBytes = (int)4e6;

    /**
     * The size, in bytes, used for the object buffer of new TCP connections. Object buffers are used
     * to hold the bytes for a single object graph until it can be sent over the network or deserialized.
     */
    private static final int defaultObjectBufferSizeBytes = (int)4e6;

    /**
     * The maximum size, in bytes, that can be used for a TCP write buffer or a TCP object buffer.
     *
     * If we find that we're trying to write data that is larger than the buffer(s), then we change the
     * size of the buffers for future TCP connections to hopefully avoid the problem. This variable sets a hard limit
     * on the maximum size of a buffer.
     */
    private static final int maxBufferSize = (int)8e6;

    /**
     * Used to redirect write operations to authorized deployments.
     */
    private final String functionUriBase;

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
        this.functionUriBase = conf.get(DFSConfigKeys.SERVERLESS_ENDPOINT, DFSConfigKeys.SERVERLESS_ENDPOINT_DEFAULT);

        this.maximumConnections = calculateMaxNumberTcpConnections();

        this.tcpClients = Caffeine.newBuilder()
                .maximumSize(maximumConnections)
                .initialCapacity(maximumConnections)
                // Close TCP clients when they are removed.
                .evictionListener((RemovalListener<ServerlessHopsFSClient, Client>) (serverlessHopsFSClient, client, removalCause) -> {
                    if (client == null)
                        return;

                    if (client.isConnected())
                        client.close();
                })
                .build();

        this.writeBufferSize = defaultWriteBufferSizeBytes;
        this.objectBufferSize = defaultObjectBufferSizeBytes;

        LOG.debug("Created NameNodeTCPClient(NN ID=" + nameNodeId + ", deployment#=" + deploymentNumber +
                ", functionUriBase=" + functionUriBase + ", writeBufferSize=" + writeBufferSize +
                " bytes, objectBufferSize=" + objectBufferSize + " bytes).");
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
        int combinedBufferSize = maxBufferSize << 2;

        // For now, we reserve 65% of the function's RAM for TCP connection buffers.
        double memoryReservedForTCPConnectionBuffers = 0.50;
        int memoryAvailableForConnections = (int) Math.floor(memoryReservedForTCPConnectionBuffers * actionMemory);

        int maximumConnections = Math.floorDiv(memoryAvailableForConnections, combinedBufferSize);
        return maximumConnections;
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

            if (tcpClient.isConnected()) {
                LOG.debug("ServerlessHopsFSClient " + serverlessHopsFSClient.getClientId() + " is still connected.");
            } else {
                LOG.debug("ServerlessHopsFSClient " + serverlessHopsFSClient.getClientId()
                        + " is DISCONNECTED.");
                toRemove.add(serverlessHopsFSClient);
            }
        }

        if (toRemove.size() > 0) {
            LOG.warn("Found " + toRemove.size() + "ServerlessHopsFSClient instance(s) that were disconnected.");

            for (ServerlessHopsFSClient hopsFSClient : toRemove)
                tcpClients.asMap().remove(hopsFSClient);
        } else {
            LOG.debug("Found 0 disconnected ServerlessHopsFSClients.");
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

        LOG.debug("Adding new TCP Client: " + newClient + ". Existing number of clients: " +
                tcpClients.estimatedSize() +
                ". Existing clients: " + StringUtils.join(tcpClients.asMap().keySet(), ", ") +
                ", current heap size: " + (Runtime.getRuntime().totalMemory() / 1000000.0) +
                " MB, free space in heap: " + (Runtime.getRuntime().freeMemory() / 1000000.0) + " MB.");

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

        // Add an entry to the TCP Clients map now so that we do not try to connect again while we're
        // in the process of connecting.
        Client existingClient = tcpClients.asMap().putIfAbsent(newClient, tcpClient);
        if (existingClient != null) {
            LOG.warn("Actually, NameNodeTCPClient already has a connection to client " + newClient);
            return false;
        }

        // Start the client in its own thread.
        // new Thread(tcpClient).start();
        tcpClient.start();

        // We need to register whatever classes will be serialized BEFORE any network activity is performed.
        ServerlessClientServerUtilities.registerClassesToBeTransferred(tcpClient.getKryo());

        tcpClient.addListener(new Listener() {
            /**
             * This listener is responsible for handling messages received from HopsFS clients. These messages will
             * generally be file system operation requests/directions. We will extract the information about the
             * operation, create a task, submit the task to be executed by our worker thread, then return the result
             * back to the HopsFS client.
             * @param connection The connection with the HopsFS client.
             * @param object The object that the client sent to us.
             */
            public void received(Connection connection, Object object) {
                NameNodeResult tcpResult;
                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof String) {
                    LOG.debug("[TCP Client] NN " + nameNodeId + " Received work assignment from " +
                                    connection.getRemoteAddressTCP() + ". current heap size: " +
                                    (Runtime.getRuntime().totalMemory() / 1000000.0) +  "MB, free space in heap: " +
                                    (Runtime.getRuntime().freeMemory() / 1000000.0) + " MB.");
                    JsonObject jsonObject = new JsonParser().parse((String)object).getAsJsonObject();
                    tcpResult = handleWorkAssignment(jsonObject);
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
                            "[TCP Client] Received object of unexpected type from client " + tcpClient
                                    + ". Object type: " + object.getClass().getSimpleName() + ".");
                    tcpResult = new NameNodeResult(deploymentNumber, "N/A", "TCP",
                            serverlessNameNode.getId());
                    tcpResult.addException(ex);
                }

                String jsonString = new Gson().toJson(tcpResult.toJson(
                        ServerlessClientServerUtilities.OPERATION_RESULT,
                        serverlessNameNode.getNamesystem().getMetadataCache()));

                LOG.debug("[TCP Client] Sending result to client at " + tcpClient.getRemoteAddressTCP() + " now...");
                int bytesSent = tcpClient.sendTCP(jsonString);

                LOG.debug("[TCP Client] Sent " + bytesSent + " bytes to HopsFS client at " + tcpClient.getRemoteAddressTCP());

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
                        LOG.debug("[TCP Client] Increasing buffer size of future TCP connections to " +
                                objectBufferSize + " bytes.");
                    else
                        // TODO: What should we do if this occurs?
                        LOG.warn("[TCP Client] Already at the maximum buffer size for TCP connections...");
                }
            }

            public void disconnected (Connection connection) {
                LOG.warn("[TCP Client] Disconnected from HopsFS client " + newClient.getClientId() +
                        " at " + newClient.getClientIp());
                tcpClients.invalidate(newClient);
            }
        });

        // We time how long it takes to establish the TCP connection for debugging/metric-collection purposes.
        Instant connectStart = Instant.now();
        Thread connectThread
                = new Thread("Thread-ConnectTo" + newClient.getClientIp() + ":" + newClient.getClientPort()) {
            public void run() {
                try {
                    // The call to connect() may produce an IOException if it times out.
                    tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getClientPort());
                } catch (IOException ex) {
                    LOG.error("IOException encountered while trying to connect to HopsFS Client via TCP:", ex);
                }
            }
        };
        connectThread.start();
        try {
            connectThread.join();
        } catch (InterruptedException ex) {
            LOG.error("InterruptedException encountered while trying to connect to HopsFS Client via TCP:", ex);
        }
        Instant connectEnd = Instant.now();

        // Compute the duration of the TCP connection establishment.
        Duration connectDuration = Duration.between(connectStart, connectEnd);

        double connectMilliseconds = TimeUnit.NANOSECONDS.toMillis(connectDuration.getNano()) +
                TimeUnit.SECONDS.toMillis(connectDuration.getSeconds());

        if (tcpClient.isConnected()) {
            LOG.debug("Successfully established connection with client " + newClient.getClientId()
                    + " in " + connectMilliseconds + " milliseconds!");

            // Now that we've registered the classes to be transferred, we can register with the server.
            registerWithClient(tcpClient);

            return true;
        } else {
            // Remove the entry that we added from the TCP client mapping. The connection establishment failed,
            // so we need to remove the record so that we may try again in the future.
            tcpClients.invalidate(newClient);
            throw new IOException("Failed to connect to client at " + newClient.getClientIp() + ":" +
                    newClient.getClientPort());
        }
    }

    private NameNodeResult handleWorkAssignment(JsonObject args) {
        String requestId = args.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_ID).getAsString();
        String op = args.getAsJsonPrimitive(ServerlessNameNodeKeys.OPERATION).getAsString();
        JsonObject fsArgs = args.getAsJsonObject(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS);

        LOG.debug("================ TCP Message Contents ================");
        LOG.debug("Request ID: " + requestId);
        LOG.debug("Operation name: " + op);
        LOG.debug("FS operation arguments: ");
        for (Map.Entry<String, JsonElement> entry : fsArgs.entrySet())
            LOG.debug("     " + entry.getKey() + ": " + entry.getValue());
        LOG.debug("======================================================\n");

        NameNodeResult tcpResult = new NameNodeResult(deploymentNumber, requestId, "TCP",
                serverlessNameNode.getId());

        tcpResult.setFnStartTime(Time.getUtcTime());

        // Create a new task. After this, we assign it to the worker thread and wait for the
        // result to be computed before returning it to the user.
        Future<Serializable> newTask = null;
        try {
            newTask = FileSystemTaskUtils.createAndEnqueueFileSystemTask(requestId, op, fsArgs, tcpResult,
                    serverlessNameNode, false);
        } catch (Exception ex) {
            LOG.error("Error encountered while creating file system task "
                    + requestId + " for operation '" + op + "':", ex);
           tcpResult.addException(ex);
            // We don't want to continue as we already encountered a critical error, so just return.
           return tcpResult;
        }

        // If we failed to create a new task for the desired file system operation, then we'll just throw
        // another exception indicating that we have nothing to execute, seeing as the task doesn't exist.
        if (newTask == null) {
            LOG.error("[TCP] Failed to create task for operation '" + op + "'");
            tcpResult.addException(new IllegalStateException("[TCP] Failed to create task for operation " + op));
            return tcpResult;
        }

        tcpResult.setEnqueuedTime(Time.getUtcTime());

        // Wait for the worker thread to execute the task. We'll return the result (if there is one) to the client.
        try {
            // In the case that we do have a task to wait on, we'll wait for the configured amount of time for the
            // worker thread to execute the task. If the task times out, then an exception will be thrown, caught,
            // and ultimately reported to the client. Alternatively, if the task is executed successfully, then
            // the future will resolve, and we'll be able to return a result to the client!
            LOG.debug("[TCP] Waiting for task " + requestId + " (operation = " + op + ") to be executed now...");
            Serializable fileSystemOperationResult = newTask.get(
                    ServerlessNameNode.tryGetNameNodeInstance(true).getWorkerThreadTimeoutMs(), TimeUnit.MILLISECONDS);

            LOG.debug("[TCP] Adding result from operation " + op + " to response for request " + requestId);
            if (fileSystemOperationResult instanceof NameNodeResult) {
                LOG.debug("[TCP] Merging NameNodeResult instances now...");
                tcpResult.mergeInto((NameNodeResult)fileSystemOperationResult, false);
            } else if (fileSystemOperationResult != null) {
                LOG.warn("[TCP] Worker thread returned result of type: "
                        + fileSystemOperationResult.getClass().getSimpleName());
                tcpResult.addResult(fileSystemOperationResult, true);
            } else {
                // This will be caught immediately and added to the result returned to the client.
                throw new IllegalStateException("Did not receive a result from the execution of task " + requestId);
            }
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName() + " while waiting for task " + requestId
                    + " to be executed by the worker thread: ", ex);
            tcpResult.addException(ex);
            // We don't have to return here as the next instruction is to return.
        }

        tcpResult.setFnEndTime(Time.getUtcTime());
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

        LOG.debug("Registering with HopsFS client at " + tcpClient.getRemoteAddressTCP() + " now...");
        int bytesSent = tcpClient.sendTCP(registration.toString());
        LOG.debug("Sent " + bytesSent + " bytes to HopsFS client at " +  tcpClient.getRemoteAddressTCP() +
                " during registration.");
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
