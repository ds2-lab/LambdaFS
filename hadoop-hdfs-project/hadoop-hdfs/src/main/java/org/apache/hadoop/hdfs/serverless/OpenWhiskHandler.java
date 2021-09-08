package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessUtilities;
import org.apache.hadoop.hdfs.serverless.tcpserver.ServerlessHopsFSClient;
import org.apache.hadoop.ipc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.google.common.hash.Hashing.consistentHash;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_TCP_SERVER_PORT_DEFAULT;

/**
 * This class encapsulates the behavior and functionality of the serverless function handler for OpenWhisk.
 *
 * The handler itself is actually just a single function, but there are lots of things that this handler does,
 * and it is cleaner to encapsulate all of this into a single class than to include it in the
 * ServerlessNameNode class directly, as was done previously.
 */
public class OpenWhiskHandler {
    public static final Logger LOG = LoggerFactory.getLogger(OpenWhiskHandler.class.getName());

    /**
     * The singleton ServerlessNameNode instance associated with this container. There can only be one!
     */
    private static ServerlessNameNode instance;

    /**
     * Used internally to determine whether this instance is warm or cold.
     */
    private static boolean isCold = true;

    /**
     * Returns the singleton ServerlessNameNode instance.
     *
     * @param commandLineArguments The command-line arguments given to the NameNode during initialization.
     * @param functionName The name of this particular OpenWhisk action.
     */
    public static ServerlessNameNode getOrCreateInstance(String[] commandLineArguments, String functionName)
            throws Exception {
        if (instance != null) {
            LOG.debug("Using existing NameNode instance.");
            return instance;
        }

        if (!isCold)
            throw new IllegalStateException("Container is warm, but there is no existing ServerlessNameNode instance.");

        instance = ServerlessNameNode.startServerlessNameNode(commandLineArguments, functionName);
        instance.populateOperationsMap();
        return instance;
    }

    /**
     * OpenWhisk handler.
     */
    public static JsonObject main(JsonObject args) {
        LOG.info("============================================================");
        LOG.info("Serverless NameNode v" + ServerlessNameNode.versionNumber + " has started executing.");
        LOG.info("============================================================\n");

        performStaticInitialization();
        String functionName = platformSpecificInitialization();

        // The arguments passed by the user are included under the 'value' key.
        JsonObject userArguments = args.get("value").getAsJsonObject();

        String clientIpAddress = null;
        if (userArguments.has("clientInternalIp")) {
            clientIpAddress = userArguments.getAsJsonPrimitive("clientInternalIp").getAsString();
        }

        String[] commandLineArguments;
        // Attempt to extract the command-line arguments, which will be passed as a single string parameter.
        if (userArguments.has("command-line-arguments")) {
            try {
                commandLineArguments = userArguments.getAsJsonPrimitive("command-line-arguments")
                        .getAsString().split("\\s+");
            } catch (ClassCastException ex) {
                // If it was included as a JsonArray, then unpack it that way.
                JsonArray commandLineArgumentsJson = userArguments.getAsJsonArray("command-line-arguments");
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

        String clientName = userArguments.getAsJsonPrimitive("clientName").getAsString();
        String operation = userArguments.getAsJsonPrimitive("op").getAsString();
        String requestId = userArguments.getAsJsonPrimitive("requestId").getAsString();
        JsonObject fsArgs = userArguments.getAsJsonObject("fsArgs");
        boolean isClientInvoker = userArguments.getAsJsonPrimitive("isClientInvoker").getAsBoolean();

        LOG.info("=-=-=-=-=-=-= Serverless Function Information =-=-=-=-=-=-=");
        LOG.debug("Top-level OpenWhisk arguments: " + args);
        LOG.debug("User-passed OpenWhisk arguments: " + userArguments);
        LOG.info("Serverless function name: " + functionName);
        LOG.info("Invoked by: " + (isClientInvoker ? "CLIENT" : "DATANODE"));
        LOG.info("Client's name: " + clientName);
        LOG.info("Client IP address: " + (clientIpAddress == null ? "N/A" : clientIpAddress));
        LOG.info("Function container was " + (isCold ? "COLD" : "WARM") + ".");
        LOG.info("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
        LOG.info("Operation name: " + clientName);
        LOG.debug("Operation arguments: " + fsArgs);
        LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");

        // Execute the desired operation. Capture the result to be packaged and returned to the user.
        JsonObject result = driver(operation, fsArgs, commandLineArguments, functionName, clientIpAddress,
                requestId, clientName, isClientInvoker);

        // Set the `isCold` flag to false given this is now a warm container.
        isCold = false;

        return createJsonResponse(result);
    }

    /**
     * Executes the NameNode code/operation/function execution.
     * @param op The name of the FS operation to be performed.
     * @param fsArgs The arguments to be passed to the desired FS operation.
     * @param commandLineArguments The command-line arguments to be consumed by the NameNode during its creation.
     * @param functionName The name of this particular OpenWhisk action (i.e., serverless function).
     * @param clientIPAddress The IP address of the client who invoked this OpenWhisk action.
     * @param requestId The OpenWhisk request ID for this particular action activation.
     * @param clientName The name of the client who invoked this OpenWhisk action. Comes from the DFSClient class.
     * @param isClientInvoker Flag which indicates whether this activation was invoked by a client or a DataNode.
     * @return Result of executing NameNode code/operation/function execution.
     */
    private static JsonObject driver(String op, JsonObject fsArgs, String[] commandLineArguments,
                                     String functionName, String clientIPAddress, String requestId,
                                     String clientName, boolean isClientInvoker) {
        NameNodeResult result = new NameNodeResult();

        // The very first step is to obtain a reference to the singleton ServerlessNameNode instance.
        // If this container was cold prior to this invocation, then we'll actually be creating the instance now.
        ServerlessNameNode serverlessNameNode;
        try {
            serverlessNameNode = getOrCreateInstance(commandLineArguments, functionName);
        }
        catch (Exception ex) {
            LOG.error("Exception encountered while creating and initializing the NameNode: ", ex);
            result.addException(ex);
            return result.toJson();
        }

        // Check to see if this is a duplicate request, in which case we should not return anything of substance.
        if (serverlessNameNode.checkIfRequestProcessedAlready(requestId)) {
            LOG.warn("This request (" + requestId + ") has already been processed. Returning now...");
            return result.toJson();
        }

        // Now we need to process various updates that are stored in intermediate storage by DataNodes.
        // These include storage reports, block reports, and new DataNode registrations.
        try {
            serverlessNameNode.getAndProcessUpdatesFromIntermediateStorage();
        }
        catch (IOException | ClassNotFoundException ex) {
            LOG.error("Encountered exception while retrieving and processing updates from intermediate storage: ", ex);
            result.addException(ex);
        }

        // Next, the NameNode needs to exit safe mode (if it is in safe mode).
        try {
            if (serverlessNameNode.isInSafeMode()) {
                LOG.debug("NameNode is in SafeMode. Leaving SafeMode now...");
                serverlessNameNode.getNamesystem().leaveSafeMode();
            }
        }
        catch (IOException ex) {
            LOG.error("Encountered exception while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        }

        // Finally, we perform the desired file system operation as specified by whoever invoked us.
        try {
            serverlessNameNode.performOperation(op, fsArgs, result);
        }
        catch (IOException ex) {
            LOG.error("Encountered IOException while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        } catch (ClassNotFoundException ex) {
            LOG.error("Encountered ClassNotFoundException while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        } catch (InvocationTargetException ex) {
            LOG.error("Encountered InvocationTargetException while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        } catch (NoSuchMethodException ex) {
            LOG.error("Encountered NoSuchMethodException while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        } catch (IllegalAccessException ex) {
            LOG.error("Encountered IllegalAccessException while checking status of/exiting safe mode: ", ex);
            result.addException(ex);
        }

        // After performing the desired FS operation, we check if there is a particular file or directory
        // identified by the `src` parameter. This is the file/directory that should be hashed to a particular
        // serverless function. We calculate this here and include the information for the client in our response.
        if (fsArgs != null && fsArgs.has("src")) {
            String src = fsArgs.getAsJsonPrimitive("src").getAsString();

            INode iNode = null;
            try {
                iNode = serverlessNameNode.getINodeForCache(src);
            }
            catch (IOException ex) {
                LOG.error("Encountered IOException while retrieving INode associated with target directory "
                        + src + ": ", ex);
                result.addException(ex);
            }

            // If we just deleted this INode, then it will presumably be null, so we need to check that it is not null.
            if (iNode != null) {
                LOG.debug("Parent INode ID for " + '"' + src + '"' + ": " + iNode.getParentId());

                int functionNumber = consistentHash(iNode.getParentId(), serverlessNameNode.getNumUniqueServerlessNameNodes());

                LOG.debug("Consistently hashed parent INode ID " + iNode.getParentId()
                        + " to serverless function " + functionNumber);

                result.addFunctionMapping(src, iNode.getParentId(), functionNumber);
            }
        }

        // The last step is to establish a TCP connection to the client that invoked us.
        if (isClientInvoker) {
            ServerlessHopsFSClient serverlessHopsFSClient = new ServerlessHopsFSClient(
                    clientName, clientIPAddress, SERVERLESS_TCP_SERVER_PORT_DEFAULT);

            try {
                serverlessNameNode.getNameNodeTcpClient().addClient(serverlessHopsFSClient);
            }
            catch (IOException ex) {
                LOG.error("Encountered IOException while trying to establish TCP connection with client: ", ex);
                result.addException(ex);
            }
        }

        result.logResultDebugInformation();
        serverlessNameNode.designateRequestAsProcessed(requestId);
        return result.toJson();
    }

    /**
     * Add an exception to the result (which is returned to whoever invoked us).
     * @param result The JsonObject encapsulating the result to be returned to the invoker.
     * @param t The exception to add to the result.
     */
    private static void addExceptionToResult(JsonObject result, Throwable t) {
        JsonArray exceptions;
        if (result.has("EXCEPTIONS"))
            exceptions = result.get("EXCEPTIONS").getAsJsonArray();
        else
            exceptions = new JsonArray();

        exceptions.add(t.toString());
    }

    /**
     * Create and return the response to return to whoever invoked this Serverless NameNode.
     * @param result The result returned from by `nameNodeDriver()`.
     * @return JsonObject to return as result of this OpenWhisk activation (i.e., serverless function execution).
     */
    private static JsonObject createJsonResponse(JsonObject result) {
        JsonObject response = new JsonObject();
        JsonObject headers = new JsonObject();
        headers.addProperty("content-type", "application/json");

        if (result.has("EXCEPTION")) {
            response.addProperty("statusCode", 422);
            response.addProperty("status", "exception");
            response.addProperty("success", false);
        }
        else if (response.has("RESULT")) {
            response.addProperty("statusCode", 200);
            response.addProperty("status", "success");
            response.addProperty("success", true);
        }
        else {
            // https://stackoverflow.com/a/3290369/5937661
            // "The request could not be completed due to a conflict with the current state of the resource."
            response.addProperty("statusCode", 409);
            response.addProperty("status", "unknown_failure");
            response.addProperty("success", false);
        }

        response.add("headers", headers);
        response.add("body", result);
        return response;
    }

    /**
     * Perform some standard start-up procedures, set as setting certain environment variables.
     *
     * I am aware that static initialization blocks exist, but I prefer to use methods.
     */
    private static void performStaticInitialization() {
        System.setProperty("sun.io.serialization.extendedDebugInfo", "true");

        if (LOG.isDebugEnabled())
            LOG.info("Debug-logging IS enabled.");
        else
            LOG.info("Debug-logging is NOT enabled.");
    }

    /**
     * In this case, we are performing OpenWhisk-specific initialization.
     *
     * @return The name of this particular OpenWhisk serverless function/action.
     */
    private static String platformSpecificInitialization() {
        String activationId = System.getenv("__OW_ACTIVATION_ID");

        LOG.debug("Hadoop configuration directory: " + System.getenv("HADOOP_CONF_DIR"));
        LOG.debug("OpenWhisk activation ID: " + activationId);

        if (ServerlessNameNode.nameNodeID == -1) {
            ServerlessNameNode.nameNodeID = ServerlessUtilities.hash(activationId);
            LOG.debug("Set name node ID to " + ServerlessNameNode.nameNodeID);
        } else {
            LOG.debug("Name node ID already set to " + ServerlessNameNode.nameNodeID);
        }

        return System.getenv("__OW_ACTION_NAME");
    }
}
