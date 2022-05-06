package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mysql.clusterj.ClusterJHelper;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments.JsonTaskArguments;
import org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments.TaskArguments;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResultWithMetrics;
import org.apache.hadoop.hdfs.serverless.tcpserver.NameNodeTcpUdpClient;
import org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessHopsFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;

/**
 * This class encapsulates the behavior and functionality of the serverless function handler for OpenWhisk.
 *
 * The handler itself is actually just a single function, but there are lots of things that this handler does,
 * and it is cleaner to encapsulate all of this into a single class than to include it in the
 * ServerlessNameNode class directly, as was done previously.
 *
 * This is used on the NameNode side (obviously).
 */
public class OpenWhiskHandler extends BaseHandler {
    //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(OpenWhiskHandler.class);

    /**
     * Used internally to determine whether this instance is warm or cold.
     */
    private static boolean isCold = true;

    //public static AtomicInteger activeRequestCounter = new AtomicInteger(0);

    static {
        System.setProperty("sun.io.serialization.extendedDebugInfo", "true");
    }

    /**
     * OpenWhisk handler.
     */
    public static JsonObject main(JsonObject args) {
        long startTime = System.currentTimeMillis();
        String functionName = platformSpecificInitialization();

        LOG.info("============================================================");
        LOG.info(functionName + " v" + ServerlessNameNode.versionNumber + " received HTTP request.");
        //int activeRequests = activeRequestCounter.incrementAndGet();
        //LOG.info("Active HTTP requests: " + activeRequests);
        LOG.info("============================================================\n");

        int actionMemory;
        JsonObject userArguments;
        if (args.has("LOCAL_MODE")) {
            LOG.debug("LOCAL MODE IS ENABLED.");
            localModeEnabled = true;

            // In this case, the top-level arguments are in-fact the user arguments.
            userArguments = args;

            // In this case, we retrieve the action memory from an environment variable.
            actionMemory = Integer.parseInt(System.getenv("__ACTION_MEMORY"));
        } else {
            // The arguments passed by the user are included under the 'value' key.
            // TODO: This may be included or not depending on the platform. If it is Nuclio, then
            //       we'll probably set it as an environment variable going forward. Just going to
            //       hard-code it for now, though.
            actionMemory = 1280; // args.get(ServerlessNameNodeKeys.ACTION_MEMORY).getAsInt();
            userArguments = args.get(ServerlessNameNodeKeys.VALUE).getAsJsonObject();
        }

        String clientIpAddress = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.CLIENT_INTERNAL_IP).getAsString();

        boolean tcpEnabled = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.TCP_ENABLED).getAsBoolean();
        boolean udpEnabled = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.UDP_ENABLED).getAsBoolean();
        boolean benchmarkModeEnabled = userArguments.getAsJsonPrimitive(BENCHMARK_MODE).getAsBoolean();

        Instant start = Instant.now();

        String[] commandLineArguments;
        // Attempt to extract the command-line arguments, which will be passed as a single string parameter.
        if (userArguments.has(ServerlessNameNodeKeys.COMMAND_LINE_ARGS)) {
            try {
                commandLineArguments = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.COMMAND_LINE_ARGS)
                        .getAsString().split("\\s+");
            } catch (ClassCastException ex) {
                // If it was included as a JsonArray, then unpack it that way.
                JsonArray commandLineArgumentsJson = userArguments.getAsJsonArray(
                        ServerlessNameNodeKeys.COMMAND_LINE_ARGS);
                commandLineArguments = new String[commandLineArgumentsJson.size()];

                // Unpack the arguments.
                for (int i = 0; i < commandLineArgumentsJson.size(); i++) {
                    commandLineArguments[i] = commandLineArgumentsJson.get(i).getAsString();
                }
            }
        }
        else {
            // Empty arguments.
            commandLineArguments = new String[0];
        }

        boolean debugEnabled = false;
        String debugString = null;

        // Check if NDB debugging is enabled. If so, then attempt to extract the dbug string. If
        // NDB debugging is enabled and a dbug string was extracted, then pass it to the ClusterJ API,
        // which will in turn pass the dbug string to the underlying NDB API.
        if (userArguments.has(ServerlessNameNodeKeys.DEBUG_NDB))
            debugEnabled = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.DEBUG_NDB).getAsBoolean();

        if (debugEnabled && userArguments.has(ServerlessNameNodeKeys.DEBUG_STRING_NDB))
            debugString = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.DEBUG_STRING_NDB).getAsString();

        if (debugEnabled && debugString != null) {
            LOG.debug("NDB debugging has been enabled. Using dbug string \"" + debugString + "\"");

            // I am not sure if we need to make sure this is not called concurrently, but just in case...
            synchronized (OpenWhiskHandler.class) {
                ClusterJHelper.newDbug().push(debugString);
            }

            LOG.debug("Also enabling ClusterJ debug logging...");
            LogManager.getLogger("com.mysql.clusterj").setLevel(Level.DEBUG);
        }

        // The name of the client. This originates from the DFSClient class.
        String clientName = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.CLIENT_NAME).getAsString();

        // The name of the file system operation that the client wants us to perform.
        String operation = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.OPERATION).getAsString();

        // This is NOT the OpenWhisk activation ID. The request ID originates at the client who invoked us.
        // That way, the corresponding TCP request (to this HTTP request) would have the same request ID.
        // This is used to prevent duplicate requests from being processed.
        String requestId = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_ID).getAsString();

        // The arguments to the file system operation.
        JsonObject fsArgs = userArguments.getAsJsonObject(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS);
        
        if (userArguments.has(LOG_LEVEL)) {
            int logLevel = userArguments.get(LOG_LEVEL).getAsInt();
            // LOG.debug("Setting log4j log level to: " + logLevel + ".");

            LogManager.getRootLogger().setLevel(getLogLevelFromInteger(logLevel));
        }

        if (userArguments.has(CONSISTENCY_PROTOCOL_ENABLED)) {
            ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL = userArguments.get(CONSISTENCY_PROTOCOL_ENABLED).getAsBoolean();
//            LOG.debug("Consistency protocol is " +
//                    (ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL ? "ENABLED." : "DISABLED."));
        }

        int tcpPort = -1;
        if (userArguments.has(ServerlessNameNodeKeys.TCP_PORT))
            tcpPort = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.TCP_PORT).getAsInt();

        int udpPort = -1;
        if (userArguments.has(ServerlessNameNodeKeys.UDP_PORT))
            udpPort = userArguments.getAsJsonPrimitive(ServerlessNameNodeKeys.UDP_PORT).getAsInt();

        // Flag that indicates whether this action was invoked by a client or a DataNode.
        boolean isClientInvoker = userArguments.getAsJsonPrimitive(
                ServerlessNameNodeKeys.IS_CLIENT_INVOKER).getAsBoolean();
        String invokerIdentity = userArguments.getAsJsonPrimitive(
                ServerlessNameNodeKeys.INVOKER_IDENTITY).getAsString();

        LOG.info("=-=-=-=-=-=-= Serverless Function Information =-=-=-=-=-=-=");
        LOG.info("Serverless function name: " + functionName);
        LOG.info("Operation name: " + operation);
        if (LOG.isDebugEnabled()) {
            LOG.info("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
            LOG.debug("Top-level OpenWhisk arguments: " + args);
            LOG.debug("User-passed OpenWhisk arguments: " + userArguments);
            LOG.debug("Benchmarking Mode: " + (benchmarkModeEnabled ? "ENABLED" : "DISABLED"));
            LOG.debug((tcpEnabled ? "TCP Enabled." : "TCP Disabled."));
            LOG.debug(udpEnabled ? "UDP Enabled." : "UDP Disabled.");
            LOG.debug("TCP Port: " + tcpPort + ", UDP Port: " + udpPort);
            LOG.debug("Action memory: " + actionMemory + "MB");
            LOG.debug("Local mode: " + (localModeEnabled ? "ENABLED" : "DISABLED"));
            LOG.debug("Client's name: " + clientName);
            LOG.debug("Client IP address: " + (clientIpAddress == null ? "N/A" : clientIpAddress));
            LOG.debug("Invoked by: " + invokerIdentity);
            LOG.debug("Function container was " + (isCold ? "COLD" : "WARM") + ".");
            LOG.debug("Operation arguments: " + fsArgs);
            LOG.debug("Handing control over to driver() function after " + DurationFormatUtils.formatDurationHMS((Duration.between(start, Instant.now()).toMillis())));
        }
        LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");

        // Execute the desired operation. Capture the result to be packaged and returned to the user.
        NameNodeResult result = driver(operation, fsArgs, commandLineArguments, functionName, clientIpAddress,
                requestId, clientName, isClientInvoker, tcpEnabled, udpEnabled, tcpPort, udpPort, actionMemory,
                startTime, benchmarkModeEnabled);

        // Set the `isCold` flag to false given this is now a warm container.
        isCold = false;

        if (LOG.isDebugEnabled()) {
            long endTime = System.currentTimeMillis();
            LOG.debug("Returning back to client. Time elapsed: " + (endTime - startTime) + " milliseconds.");
            LOG.debug("ServerlessNameNode is exiting now...");
        }
        //activeRequestCounter.decrementAndGet();
        return createJsonResponse(result);
    }

    /**
     * Assert that the container was cold prior to the currently-running activation.
     *
     * If the container is warm, then this will create a new IllegalStateException and add it to the
     * parameterized `result` object so that the client that invoked this function can see the exception
     * and process it accordingly.
     * @param result The result to be returned to the client.
     * @return True if the function was cold (as is expected when this function is called). Otherwise, false will be
     * returned, indicating that the function is warm (which is bad in the case that this function is called).
     */
    private static boolean assertIsCold(NameNodeResult result) {
        if (!isCold) {
            // Create and add the exception to the result so that it will be returned to the client.
            IllegalStateException ex = new IllegalStateException("Expected container to be cold, but it is warm.");
            result.addException(ex);

            return false;
        }

        return true;
    }

    /**
     * Executes the NameNode code/operation/function execution.
     * @param op The name of the FS operation to be performed.
     * @param fsArgs The arguments to be passed to the desired FS operation.
     * @param commandLineArguments The command-line arguments to be consumed by the NameNode during its creation.
     * @param functionName The name of this particular OpenWhisk action (i.e., serverless function).
     * @param clientIPAddress The IP address of the client who invoked this OpenWhisk action.
     * @param requestId The request ID for this particular action activation. This is NOT the OpenWhisk activation ID.
     *                  The request ID originates at the client who invoked us. That way, the corresponding TCP request
     *                  (to this HTTP request) would have the same request ID. This is used to prevent duplicate
     *                  requests from being processed.
     * @param clientName The name of the client who invoked this OpenWhisk action. Comes from the DFSClient class.
     * @param isClientInvoker Flag which indicates whether this activation was invoked by a client or a DataNode.
     * @param tcpPort The TCP port that the client who invoked us is using for their TCP server.
     *                If the client is using multiple HopsFS client threads at the same time, then there will
     *                presumably be multiple TCP servers, each listening on a different TCP port.
     * @param actionMemory The amount of RAM (in megabytes) that this function has been allocated. Used when
     *                     determining the number of active TCP connections that this NameNode can have at once, as
     *                     each TCP connection has two relatively-large buffers. If the NN creates too many TCP
     *                     connections at once, then it might crash due to OOM errors.
     * @param tcpEnabled Flag indicating whether the TCP invocation pathway is enabled. If false, then we do not
     *                   bother trying to establish TCP connections.
     * @param localMode Indicates whether we're being executed in a local container for testing/profiling/debugging purposes.
     * @param startTime Return value from System.currentTimeMillis() called as the VERY first thing the HTTP handler does.
     * @return Result of executing NameNode code/operation/function execution.
     */
    private static NameNodeResult driver(String op, JsonObject fsArgs, String[] commandLineArguments,
                                         String functionName, String clientIPAddress, String requestId,
                                         String clientName, boolean isClientInvoker, boolean tcpEnabled,
                                         boolean udpEnabled, int tcpPort, int udpPort, int actionMemory,
                                         long startTime, boolean benchmarkModeEnabled) {
        NameNodeResult result;

        if (benchmarkModeEnabled) {
            LOG.debug("Creating instance of NameNodeResult (i.e., NO metrics).");
            result = new NameNodeResult(requestId, op);
        } else {
            LOG.debug("Creating instance of NameNodeResultWithMetrics.");
            result = new NameNodeResultWithMetrics(ServerlessNameNode.getFunctionNumberFromFunctionName(functionName),
                    requestId, "HTTP", -1, op);
        }

        if (LOG.isDebugEnabled()) LOG.debug("======== Getting or Creating Serverless NameNode Instance ========");

        long creationStart = System.currentTimeMillis();
        // The very first step is to obtain a reference to the singleton ServerlessNameNode instance.
        // If this container was cold prior to this invocation, then we'll actually be creating the instance now.
        ServerlessNameNode serverlessNameNode;
        try {
            serverlessNameNode = ServerlessNameNode.getOrCreateNameNodeInstance(commandLineArguments, functionName,
                    actionMemory, isCold);
        }
        catch (Exception ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName()
                    + " while creating and initializing the NameNode: ", ex);
            result.addException(ex);
            return result;
        }

        long creationEnd = System.currentTimeMillis();
        long creationDuration = creationEnd - creationStart;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtained NameNode instance with ID=" + serverlessNameNode.getId());
            LOG.debug("Getting/creating NN instance took: " + DurationFormatUtils.formatDurationHMS(creationDuration));
            LOG.debug("==================================================================");
        }

        // Check if we need to redo this operation. This can occur if the TCP connection that was supposed
        // to deliver the result back to the client was dropped before the client received the result.
        boolean redoEvenIfDuplicate = fsArgs.has(FORCE_REDO) && fsArgs.get(FORCE_REDO).getAsBoolean();

        // Finally, create a new task and assign it to the worker thread.
        // After this, we will simply wait for the result to be completed before returning it to the user.
        // FileSystemTask<Serializable> task = new FileSystemTask<>(requestId, op, fsArgs, redoEvenIfDuplicate, "HTTP");

        currentRequestId.set(requestId);

        // Wait for the worker thread to execute the task. We'll return the result (if there is one) to the client.
        try {
            if (result instanceof NameNodeResultWithMetrics)
                serverlessNameNode.getExecutionManager().tryExecuteTask(
                        requestId, op, new JsonTaskArguments(fsArgs), redoEvenIfDuplicate, (NameNodeResultWithMetrics)result, true);
            else
                serverlessNameNode.getExecutionManager().tryExecuteTask(
                        requestId, op, new JsonTaskArguments(fsArgs), redoEvenIfDuplicate, result, true);
        } catch (Exception ex) {
            LOG.error("Encountered " + ex.getClass().getSimpleName() + " while waiting for task " + requestId
                    + " to be executed by the worker thread: ", ex);
            result.addException(ex);
        }

        // The last step is to establish a TCP connection to the client that invoked us.
        if (isClientInvoker && tcpEnabled) {
            ServerlessHopsFSClient serverlessHopsFSClient = new ServerlessHopsFSClient(
                    clientName, clientIPAddress, tcpPort, udpPort, udpEnabled);

            final NameNodeTcpUdpClient tcpClient = serverlessNameNode.getNameNodeTcpClient();
            // Do this in a separate thread so that we can return the result back to the user immediately.
            new Thread(() -> {
                try {
                    if (LOG.isDebugEnabled())LOG.debug("Attempting to connect to client " + serverlessHopsFSClient +
                            " in separate thread.");
                    tcpClient.addClient(serverlessHopsFSClient);
                } catch (IOException ex) {
                    LOG.error("Encountered exception while connecting to client " + serverlessHopsFSClient + ":", ex);
                }
            }).start();
        }

        if (!benchmarkModeEnabled) {
            NameNodeResultWithMetrics resultWithMetrics = (NameNodeResultWithMetrics)result;
            // isCold is still equal to its original value here, which would be 'true' if this was in fact a cold start.
            resultWithMetrics.setColdStart(isCold);
            resultWithMetrics.setFnStartTime(startTime);
            resultWithMetrics.setNameNodeId(serverlessNameNode.getId());
            resultWithMetrics.logResultDebugInformation(op);
        }
        return result;
    }

    public static org.apache.log4j.Level getLogLevelFromString(String level) {
       if (level.equalsIgnoreCase("info"))
           return Level.INFO;
       else if (level.equalsIgnoreCase("debug"))
           return Level.DEBUG;
       else if (level.equalsIgnoreCase("warn"))
           return Level.WARN;
       else if (level.equalsIgnoreCase("error"))
           return Level.ERROR;
       else if (level.equalsIgnoreCase("trace"))
           return Level.TRACE;
       else if (level.equalsIgnoreCase("fatal"))
           return Level.FATAL;
       else if (level.equalsIgnoreCase("all"))
           return Level.ALL;

       LOG.error("Unknown log level specified: '" + level + "'. Defaulting to 'debug'.");
       return Level.DEBUG;
    }

    public static org.apache.log4j.Level getLogLevelFromInteger(int level) {
        if (level == 0)
            return Level.INFO;
        else if (level == 1)
            return Level.DEBUG;
        else if (level == 2)
            return Level.WARN;
        else if (level == 3)
            return Level.ERROR;
        else if (level == 4)
            return Level.TRACE;
        else if (level == 5)
            return Level.FATAL;
        else if (level == 6)
            return Level.ALL;

        LOG.error("Unknown log level specified: '" + level + "'. Defaulting to 'debug'.");
        return Level.DEBUG;
    }

    public static int getLogLevelIntFromString(String level) {
        if (level.equalsIgnoreCase("info"))
            return 0;
        else if (level.equalsIgnoreCase("debug"))
            return 1;
        else if (level.equalsIgnoreCase("warn"))
            return 2;
        else if (level.equalsIgnoreCase("error"))
            return 3;
        else if (level.equalsIgnoreCase("trace"))
            return 4;
        else if (level.equalsIgnoreCase("fatal"))
            return 5;
        else if (level.equalsIgnoreCase("all"))
            return 6;

        LOG.error("Unknown log level specified: '" + level + "'. Defaulting to 'debug'.");
        return 1;
    }

    /**
     * Check if there is a single target file/directory specified within the file system arguments passed by the client.
     * If there is, then we will create a function mapping for the client, so they know which NameNode is responsible
     * for caching the metadata associated with that particular file/directory.
     * @param result The current NameNodeResult, which will ultimately be returned to the user.
     * @param fsArgs The file system operation arguments passed in by the client (i.e., the individual who invoked us).
     * @param serverlessNameNode The current serverless name node instance, as we need this to determine the mapping.
     */
    public static void tryCreateDeploymentMapping(NameNodeResult result,
                                                  TaskArguments fsArgs,
                                                  ServerlessNameNode serverlessNameNode)
            throws IOException {
        // After performing the desired FS operation, we check if there is a particular file or directory
        // identified by the `src` parameter. This is the file/directory that should be hashed to a particular
        // serverless function. We calculate this here and include the information for the client in our response.
        if (fsArgs != null && fsArgs.contains(ServerlessNameNodeKeys.SRC)) {
            //String src = fsArgs.getAsJsonPrimitive(ServerlessNameNodeKeys.SRC).getAsString();
            String src = fsArgs.getString(SRC);

            INode inode = null;
            try {
                synchronized (serverlessNameNode) {
                    inode = serverlessNameNode.getINodeForCache(src);
                }
            }
            catch (IOException ex) {
                LOG.error("Encountered IOException while retrieving INode associated with target directory "
                        + src + ": ", ex);
                result.addException(ex);
            }

            // If we just deleted this INode, then it will presumably be null, so we need to check that it is not null.
            if (inode != null) {
                if (LOG.isDebugEnabled()) LOG.debug("Parent INode ID for '" + src + "': " + inode.getParentId());

                int functionNumber = serverlessNameNode.getMappedDeploymentNumber(inode.getParentId());

                if (LOG.isDebugEnabled()) LOG.debug("Consistently hashed parent INode ID " + inode.getParentId() + " to serverless function " + functionNumber);

                result.addFunctionMapping(src, inode.getParentId(), functionNumber);
            }
        }
    }

    /**
     * Create and return the response to return to whoever invoked this Serverless NameNode.
     * @param result The result returned from by `driver()`.
     * @return JsonObject to return as result of this OpenWhisk activation (i.e., serverless function execution).
     */
    private static JsonObject createJsonResponse(NameNodeResult result) {
        JsonObject resultJson = result.toJson(ServerlessNameNode.
                tryGetNameNodeInstance(true).getNamesystem().getMetadataCacheManager());

        JsonObject response = new JsonObject();
        JsonObject headers = new JsonObject();
        headers.addProperty("content-type", "application/json");

        // TODO: We cannot gauge whether or not a request was successful simply on the basis of whether there is/isn't
        //       a result and if there are or are not any exceptions. Certain FS operations return nothing, meaning
        //       there wouldn't be a result. And the NN can encounter exceptions but still succeed. So for now, we'll
        //       just always return a statusCode 200, but eventually we may want to create a more robust system that
        //       uses status codes to indicate failures.

        response.addProperty("statusCode", 200);
        response.addProperty("status", "success");
        response.addProperty("success", true);

        response.add("headers", headers);
        response.add("body", resultJson);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Contents of result to be returned to the client: ");
            for (String key : resultJson.keySet())
                if (key.equals(RESULT))
                    LOG.debug("Result: <Content Omitted>");
                else
                    LOG.debug(key + ": " + resultJson.get(key).toString());
        }

        return response;
    }
}
