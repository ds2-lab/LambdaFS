package org.apache.hadoop.hdfs.serverless.tcpserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
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
     * Constructor.
     * @param functionName The name of the serverless function in which this TCP client exists.
     */
    public NameNodeTCPClient(String functionName) {
        this.functionName = functionName;
        tcpClients = new HashMap<>();
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

        // The call to connect() may produce an IOException if it times out.
        Client tcpClient = new Client();
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

                NameNodeResult tcpResult = new NameNodeResult();

                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof JsonObject) {
                    handleWorkAssignment((JsonObject) object, tcpResult);
                }
                else {
                    // Create and log the exception to be returned to the client,
                    // so they know they sent the wrong thing.
                    IllegalArgumentException ex = new IllegalArgumentException(
                            "[TCP Client] Received object of unexpected type from client " + tcpClient
                                    + ". Object type: " + object.getClass().getSimpleName() + ".");
                    tcpResult.addException(ex);
                }

                connection.sendTCP(tcpResult.toJson(ServerlessClientServerUtilities.OPERATION_RESULT));
            }

            public void disconnected (Connection connection) {
                LOG.warn("[TCP Client] Disconnected from HopsFS client " + newClient.getClientId() +
                        " at " + newClient.getClientIp());
                tcpClients.remove(newClient);
            }
        });

        // We time how long it takes to establish the TCP connection for debugging/metric-collection purposes.
        Instant connectStart = Instant.now();
        tcpClient.connect(CONNECTION_TIMEOUT, newClient.getClientIp(), newClient.getClientPort());
        Instant connectEnd = Instant.now();

        // Compute the duration of the TCP connection establishment.
        Duration connectDuration = Duration.between(connectStart, connectEnd);

        double connectMilliseconds = TimeUnit.NANOSECONDS.toMillis(connectDuration.getNano()) +
                TimeUnit.SECONDS.toMillis(connectDuration.getSeconds());

        LOG.debug("Successfully established connection with client " + newClient.getClientId()
                + " in " + connectMilliseconds + " milliseconds!");

        // Now that we've registered the classes to be transferred, we can register with the server.
        registerWithClient(tcpClient);

        tcpClients.put(newClient, tcpClient);

        return true;
    }

    private void handleWorkAssignment(JsonObject args, NameNodeResult result) {
        String requestId = args.getAsJsonPrimitive("requestId").getAsString();
        String op = args.getAsJsonPrimitive("op").getAsString();
        JsonObject fsArgs = args.getAsJsonObject("fsArgs");

        LOG.debug("================ TCP Message Contents ================");
        LOG.debug("Request ID: " + requestId);
        LOG.debug("Operation name: " + op);
        LOG.debug("FS operation arguments: ");
        for (Map.Entry<String, JsonElement> entry : fsArgs.entrySet())
            LOG.debug("     " + entry.getKey() + ": " + entry.getValue());
        LOG.debug("======================================================");

        NameNodeResult tcpResult = new NameNodeResult();

        // Create a new task and assign it to the worker thread.
        // After this, we will simply wait for the result to be completed before returning it to the user.
        FileSystemTask<Serializable> newTask = null;
        try {
            LOG.debug("[TCP] Adding task " + requestId + " (operation = " + op + ") to work queue now...");
            newTask= new FileSystemTask<>(requestId, op, fsArgs);
            OpenWhiskHandler.workQueue.put(newTask);

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
            Object fileSystemOperationResult = newTask.get(
                    OpenWhiskHandler.tryGetNameNodeInstance().getWorkerThreadTimeoutMs(), TimeUnit.MILLISECONDS);

            // Serialize the resulting HdfsFileStatus/LocatedBlock/etc. object, if it exists, and encode it to Base64 so we
            // can include it in the JSON response sent back to the invoker of this serverless function.
            if (fileSystemOperationResult != null) {
                LOG.debug("[TCP] Adding result from operation " + op + " to response for request " + requestId);
                result.addResult(fileSystemOperationResult, true);
            }
        } catch (Exception ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName() + " while waiting for task " + requestId
                    + " to be executed by the worker thread: ", ex);
            result.addException(ex);
        }
    }

    /**
     * Complete the registration phase with the client's TCP server.
     * @param tcpClient The TCP connection established with the client.
     */
    private void registerWithClient(Client tcpClient) {
        // Complete the registration with the TCP server.
        JsonObject registration = new JsonObject();
        registration.addProperty("op", ServerlessClientServerUtilities.OPERATION_REGISTER);
        registration.addProperty("functionName", functionName);

        LOG.debug("Registering with HopsFS client at " + tcpClient.getRemoteAddressTCP() + " now...");
        int bytesSent = tcpClient.sendTCP(registration);
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
