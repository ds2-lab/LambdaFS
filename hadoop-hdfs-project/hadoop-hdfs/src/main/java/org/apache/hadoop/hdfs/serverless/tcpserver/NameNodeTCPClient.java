package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.operation.FileSystemTask;
import org.apache.hadoop.hdfs.serverless.operation.NameNodeResult;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private final HashMap<ServerlessHopsFSClient, Client> tcpClients;

    /**
     * The name of the serverless function in which this TCP client exists.
     */
    private final String functionName;

    /**
     * The ServerlessNameNode instance that this TCP Client is associated with.
     */
    private final ServerlessNameNode serverlessNameNode;

    /**
     * The size, in bytes, used for the write buffer of new TCP connections. Objects are serialized to
     * the write buffer where the bytes are queued until they can be written to the TCP socket.
     */
    private static final int defaultWriteBufferSizeBytes = 8192;

    /**
     * The size, in bytes, used for the object buffer of new TCP connections. Object buffers are used
     * to hold the bytes for a single object graph until it can be sent over the network or deserialized.
     */
    private static final int defaultObjectBufferSizeBytes = 4096;

    /**
     * The maximum size, in bytes, that can be used for a TCP write buffer or a TCP object buffer.
     */
    private static final int maxBufferSize = 16384;

    /**
     * The current size, in bytes, being used for TCP write buffers. If we notice a buffer overflow,
     * then we increase the size of this buffer for future TCP connections in the hopes that we'll
     * avoid a buffer overflow.
     */
    private int writeBufferSize;

    /**
     * The current size, in bytes, being used for TCP object buffers. If we notice a buffer overflow,
     * then we increase the size of this buffer for future TCP connections in the hopes that we'll
     * avoid a buffer overflow.
     */
    private int objectBufferSize;

    /**
     * Constructor.
     * @param functionName The name of the serverless function in which this TCP client exists.
     */
    public NameNodeTCPClient(String functionName, ServerlessNameNode serverlessNameNode) {
        this.functionName = functionName;
        this.serverlessNameNode = serverlessNameNode;
        this.tcpClients = new HashMap<>();

        this.writeBufferSize = defaultWriteBufferSizeBytes;
        this.objectBufferSize = defaultObjectBufferSizeBytes;
        //Log.set(Log.LEVEL_TRACE);
    }

    /**
     * Iterate over the TCP connections maintained by this client and check that all of them are still
     * connected. If any are found that are no longer connected, remove them from the list.
     *
     * This is called periodically by the worker thread.
     */
    public synchronized void checkThatClientsAreAllConnected() {
        ArrayList<ServerlessHopsFSClient> toRemove = new ArrayList<>();

        for (Map.Entry<ServerlessHopsFSClient, Client> entry : tcpClients.entrySet()) {
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
                tcpClients.remove(hopsFSClient);
        } else {
            LOG.debug("Found 0 disconnected ServerlessHopsFSClients.");
        }
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
                LOG.debug("[TCP Client] Received message from connection " + connection.toString());

                NameNodeResult tcpResult;
                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof String) {
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
                    tcpResult = new NameNodeResult(functionName, "N/A", "TCP");
                    tcpResult.addException(ex);
                }

                String jsonString = new Gson().toJson(tcpResult.toJson(
                        ServerlessClientServerUtilities.OPERATION_RESULT));

                LOG.debug("[TCP Client] Sending the following JSON string to client at " + tcpClient.getRemoteAddressTCP()
                    + ": " + jsonString);
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
                tcpClients.remove(newClient);
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

            tcpClients.put(newClient, tcpClient);

            return true;
        } else {
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

        NameNodeResult tcpResult = new NameNodeResult(functionName, requestId, "TCP");

        // Create a new task and assign it to the worker thread.
        // After this, we will simply wait for the result to be completed before returning it to the user.
        FileSystemTask<Serializable> newTask = null;
        try {
            LOG.debug("[TCP] Adding task " + requestId + " (operation = " + op + ") to work queue now...");
            newTask = new FileSystemTask<>(requestId, op, fsArgs);
            serverlessNameNode.enqueueFileSystemTask(newTask);

            // We wait for the task to finish executing in a separate try-catch block so that, if there is
            // an exception, then we can log a specific message indicating where the exception occurred. If we
            // waited for the task in this block, we wouldn't be able to indicate in the log whether the
            // exception occurred when creating/scheduling the task or while waiting for it to complete.
        }
        catch (Exception ex) {
            LOG.error("[TCP] Encountered " + ex.getClass().getSimpleName()
                    + " while assigning a new task to the worker thread: ", ex);
            tcpResult.addException(ex);
        }

        // Wait for the worker thread to execute the task. We'll return the result (if there is one) to the client.
        try {
            // If we failed to create a new task for the desired file system operation, then we'll just throw
            // another exception indicating that we have nothing to execute, seeing as the task doesn't exist.
            if (newTask == null) {
                throw new IllegalStateException("[TCP] Failed to create task for operation " + op);
            }

            // In the case that we do have a task to wait on, we'll wait for the configured amount of time for the
            // worker thread to execute the task. If the task times out, then an exception will be thrown, caught,
            // and ultimately reported to the client. Alternatively, if the task is executed successfully, then
            // the future will resolve, and we'll be able to return a result to the client!
            LOG.debug("[TCP] Waiting for task " + requestId + " (operation = " + op + ") to be executed now...");
            Serializable fileSystemOperationResult = newTask.get(
                    OpenWhiskHandler.tryGetNameNodeInstance().getWorkerThreadTimeoutMs(), TimeUnit.MILLISECONDS);

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
        } catch (Exception ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName() + " while waiting for task " + requestId
                    + " to be executed by the worker thread: ", ex);
            tcpResult.addException(ex);
        }

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
        registration.addProperty(ServerlessNameNodeKeys.FUNCTION_NAME, functionName);

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
}
