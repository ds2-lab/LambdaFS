package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.MetaStatus;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.tcpserver.HopsFSUserServer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.codehaus.jackson.impl.JsonReadContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_PLATFORM_DEFAULT;

/**
 * This serves as an adapter between the DFSClient interface and the serverless NameNode API.
 *
 * This basically enables the DFSClient code to remain unmodified; it just issues its commands
 * to an instance of this class, which transparently handles the serverless invoking code.
 */
public class ServerlessNameNodeClient implements ClientProtocol {

    public static final Log LOG = LogFactory.getLog(ServerlessNameNodeClient.class);

    /**
     * Responsible for invoking the Serverless NameNode(s).
     */
    public ServerlessInvokerBase<JsonObject> serverlessInvoker;

    /**
     * Issue HTTP requests to this to invoke serverless functions.
     *
     * This is the BASE endpoint, meaning we must append a number to the end of it to reach
     * an actual function. This is because functions are named as PREFIX1, PREFIX2, ..., where
     * PREFIX is user-specified/user-configured.
     */
    public String serverlessEndpointBase;

    /**
     * The name of the serverless platform being used for the Serverless NameNodes.
     */
    public String serverlessPlatformName;

    private final DFSClient dfsClient;

    private final HopsFSUserServer tcpServer;

    /**
     * Flag that dictates whether TCP requests can be used to perform FS operations.
     */
    private final boolean tcpEnabled;

    public ServerlessNameNodeClient(Configuration conf, DFSClient dfsClient) throws IOException {
        // "https://127.0.0.1:443/api/v1/web/whisk.system/default/namenode?blocking=true";
        serverlessEndpointBase = conf.get(SERVERLESS_ENDPOINT, SERVERLESS_ENDPOINT_DEFAULT);
        serverlessPlatformName = conf.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT);
        tcpEnabled = conf.getBoolean(SERVERLESS_TCP_REQUESTS_ENABLED, SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);

        LOG.info("Serverless endpoint: " + serverlessEndpointBase);
        LOG.info("Serverless platform: " + serverlessPlatformName);
        LOG.info("TCP requests are " + (tcpEnabled ? "enabled." : "disabled."));

        this.serverlessInvoker = dfsClient.serverlessInvoker;

        // This should already be set to true in the DFSClient class.
        this.serverlessInvoker.setIsClientInvoker(true);

        this.dfsClient = dfsClient;

        this.tcpServer = new HopsFSUserServer(conf);
        this.tcpServer.startServer();
    }

    public void printDebugInformation() {
        this.tcpServer.printDebugInformation();
    }

    /**
     * Perform an HTTP invocation of a serverless name node function concurrently with a TCP request to the same
     * Serverless NameNode, if a connection to that NameNode already exists. If no such connection exists, then only
     * the HTTP request will be issued.
     *
     * @param operationName The name of the FS operation that the NameNode should perform.
     * @param serverlessEndpoint The (base) OpenWhisk URI of the serverless NameNode(s).
     * @param nameNodeArguments The command-line arguments to be given to the NN, should it be created within the NN
     *                          function container (i.e., during a cold start).
     * @param opArguments The arguments to be passed to the specified file system operation.
     * @return The result of executing the desired FS operation on the NameNode.
     */
    private JsonObject submitOperationToNameNode(String operationName,
                                                      String serverlessEndpoint,
                                                      HashMap<String, Object> nameNodeArguments,
                                                      ArgumentContainer opArguments)
            throws IOException, InterruptedException, ExecutionException {
        // Check if there's a source directory parameter, as this is the file or directory that could
        // potentially be mapped to a serverless function.
        Object sourceObject = opArguments.has(ServerlessNameNodeKeys.SRC);

        // If tcpEnabled is false, we don't even bother checking to see if we can issue a TCP request.
        if (tcpEnabled && sourceObject instanceof String) {
            String sourceFileOrDirectory = (String)sourceObject;

            // Next, let's see if we have an entry in our cache for this file/directory.
            int mappedFunctionNumber = serverlessInvoker.getFunctionNumberForFileOrDirectory(sourceFileOrDirectory);

            // If there was indeed an entry, then we need to see if we have a connection to that NameNode.
            // If we do, then we'll concurrently issue a TCP request and an HTTP request to that NameNode.
            if (mappedFunctionNumber != -1 && tcpServer.connectionExists(mappedFunctionNumber)) {
                return issueConcurrentTcpHttpRequests(
                        operationName,
                        serverlessEndpoint,
                        nameNodeArguments,
                        opArguments,
                        mappedFunctionNumber);
            } else {
                LOG.debug("Source file/directory " + sourceFileOrDirectory + " is mapped to serverless NameNode " +
                        mappedFunctionNumber + ". TCP connection exists: " +
                        tcpServer.connectionExists(mappedFunctionNumber));
            }
        }

        LOG.debug("Issuing HTTP request only for operation " + operationName);

        // If there is no "source" file/directory argument, or if there was no existing mapping for the given source
        // file/directory, then we'll just use an HTTP request.
        return dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                operationName,
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    /**
     * Concurrently issue an HTTP request and a TCP request to a particular serverless NameNode.
     * @param operationName The name of the FS operation that the NameNode should perform.
     * @param serverlessEndpoint The (base) OpenWhisk URI of the serverless NameNode(s).
     * @param nameNodeArguments The command-line arguments to be given to the NN, should it be created within the NN
     *                          function container (i.e., during a cold start).
     * @param opArguments The arguments to be passed to the specified file system operation.
     * @param mappedFunctionNumber The function number of the serverless NameNode deployment associated with the
     *                             target file or directory.
     * @return The result of executing the desired FS operation on the NameNode.
     */
    private JsonObject issueConcurrentTcpHttpRequests(String operationName,
                                                      String serverlessEndpoint,
                                                      HashMap<String, Object> nameNodeArguments,
                                                      ArgumentContainer opArguments,
                                                      int mappedFunctionNumber)
            throws InterruptedException, ExecutionException {

        long opStart = System.nanoTime();
        String requestId = UUID.randomUUID().toString();
        LOG.debug("Issuing concurrent HTTP/TCP request for operation " + operationName + " now. Request ID = "
            + requestId);

        // Create an ExecutorService to execute the HTTP and TCP requests concurrently.
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Create a CompletionService to listen for results from the futures we're going to create.
        CompletionService<JsonObject> completionService = new ExecutorCompletionService<JsonObject>(executorService);

        // Submit the TCP request here.
        completionService.submit(() -> {
            JsonObject payload = new JsonObject();
            payload.addProperty(ServerlessNameNodeKeys.REQUEST_ID, requestId);
            payload.addProperty(ServerlessNameNodeKeys.OPERATION, operationName);
            payload.add(ServerlessNameNodeKeys.FILE_SYSTEM_OP_ARGS, opArguments.convertToJsonObject());

            // We're effectively wrapping a Future in a Future here...
            return tcpServer.issueTcpRequestAndWait(mappedFunctionNumber, false, payload);
        });

        LOG.debug("Successfully submitted TCP request. Submitting HTTP request now...");

        // Submit the HTTP request here.
        completionService.submit(() -> dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                operationName,
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments,
                requestId));

        LOG.debug("Successfully submitted HTTP request. Waiting for first result now...");

        // We should NOT just return the first result that we get.
        // It is not uncommon that the NameNode will receive the TCP request first and begin working on the task.
        // Then, while the task is being executed, the NN will receive the HTTP request, notice that the associated
        // operation is already being worked on, and return a null response to the user via HTTP. The user will often
        // receive this HTTP response first. In this scenario, we should simply discard the HTTP response, as the
        // TCP response will actually contain the result of the FS operation.


        boolean receivedTcp = false;
        boolean receivedHttp = false;

        while (true) {
            LOG.debug("Waiting for a result from the RequestResponseFuture for task " + requestId + "...");
            Future<JsonObject> potentialResult = completionService.take();

            try {
                JsonObject responseJson = potentialResult.get();

                // Now we should check if this response contains a result, or is simply a duplicate request
                // notification from whichever request the NameNode received last. That is, if the NN received the
                // HTTP request second, then this could be an HTTP response indicating that the task was already
                // being executed. The same could be true in the case of the TCP response.
                //
                // If this is an actual result, then we can return it to the user. Otherwise, we must keep waiting.

                if (responseJson.has(ServerlessNameNodeKeys.DUPLICATE_REQUEST) &&
                        responseJson.getAsJsonPrimitive(ServerlessNameNodeKeys.DUPLICATE_REQUEST).getAsBoolean()) {

                    String requestMethod =
                            responseJson.getAsJsonPrimitive(ServerlessNameNodeKeys.REQUEST_METHOD).getAsString();

                    if (requestMethod.equals("TCP"))
                        receivedTcp = true;
                    else
                        receivedHttp = true;


                    LOG.debug("Received duplicate request acknowledgement via " + requestId + " from NameNode. " +
                            "Must continue waiting for real result.");

                    // Just wait for the next result.
                    //potentialResult = completionService.take();
                    //responseJson = potentialResult.get();
                    continue;
                }
                else if (responseJson.has(ServerlessNameNodeKeys.CANCELLED) &&
                        responseJson.getAsJsonPrimitive(ServerlessNameNodeKeys.CANCELLED).getAsBoolean()) {
                    LOG.debug("The TCP future for request " + requestId + " has been cancelled. Reason: " +
                            responseJson.get(ServerlessNameNodeKeys.REASON).getAsString());

                    receivedTcp = true; // We didn't technically receive it, but we're not going to so...
                    boolean shouldRetry = responseJson.get(ServerlessNameNodeKeys.SHOULD_RETRY).getAsBoolean();
                    LOG.debug("Should retry: " + shouldRetry);

                    if (receivedHttp) {
                        LOG.debug("Already received HTTP response, probably as a \"duplicate request\" notification.");

                        // TODO: Right now, we just re-submit once. We could definitely resubmit multiple times,
                        //       though, and the number of times a resubmission is attempted could be configurable.

                        LOG.debug("Resubmitting request " + requestId + "(op=" + operationName + ") via HTTP now...");

                        // TODO: If we make it so resubmission can occur more than once, we'll need to
                        //       add a check here to see if `opArguments` already has the 'redo' field.
                        opArguments.addPrimitive(ServerlessNameNodeKeys.FORCE_REDO, true);

                        // Resubmit the HTTP request.
                        completionService.submit(() -> dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                                operationName,
                                dfsClient.serverlessEndpoint,
                                // We do not have any additional/non-default arguments to pass to the NN.
                                null,
                                opArguments,
                                requestId));
                        continue;
                    } else {
                        LOG.debug("Have not yet received HTTP response. Will continue waiting...");
                        continue;
                    }
                }

                executorService.shutdown();
                long opEnd = System.nanoTime();
                long opDuration = opEnd - opStart;
                long durationMilliseconds = TimeUnit.NANOSECONDS.toMillis(opDuration);
                LOG.debug("Successfully obtained response from HTTP/TCP request for operation " +
                        operationName + " in " + durationMilliseconds + " milliseconds.");
                return responseJson;
            } catch (ExecutionException | InterruptedException ex) {
                // Log it.
                LOG.error("Encountered " + ex.getClass().getSimpleName() + " while extracting result from Future " +
                        "for operation " + operationName + ":", ex);

                // Throw it again.
                throw ex;
            }
        }
    }

    private void handleDuplicateRequest() {

    }

    /**
     * Shuts down this client. Currently, the only steps taken during shut-down is the stopping of the TCP server.
     */
    public void stop() {
        LOG.debug("ServerlessNameNodeClient stopping now...");
        this.tcpServer.stop();
    }

    @Override
    public JsonObject latencyBenchmark(String connectionUrl, String dataSource, String query, int id) throws SQLException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
        LocatedBlocks locatedBlocks = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("offset", offset);
        opArguments.put("length", length);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "getBlockLocations",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getBlockLocations to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getBlockLocations to NameNode.");
        }

        Object result = null;
        try {
            result = this.serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (result != null)
            locatedBlocks = (LocatedBlocks)result;

        return locatedBlocks;
    }

    @Override
    public LocatedBlocks getMissingBlockLocations(String filePath) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addBlockChecksum(String src, int blockIndex, long checksum) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long getBlockChecksum(String src, int blockIndex) throws IOException {
        return 0;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        FsServerDefaults serverDefaults = null;

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "getServerDefaults",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    new ArgumentContainer());
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getServerDefaults to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getServerDefaults to NameNode.");
        }

        Object result = null;
        try {
            result = this.serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (result != null)
            serverDefaults = (FsServerDefaults)result;

        return serverDefaults;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions, EncodingPolicy policy)
            throws IOException {
        // We need to pass a series of arguments to the Serverless NameNode. We prepare these arguments here
        // in a HashMap and pass them off to the ServerlessInvoker, which will package them up in the required
        // format for the Serverless NameNode.
        HdfsFileStatus stat = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("masked", masked.toShort());
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, dfsClient.clientName);

        // Convert this argument (to the 'create' function) to a String so we can send it over JSON.
        DataOutputBuffer out = new DataOutputBuffer();
        ObjectWritable.writeObject(out, flag, flag.getClass(), null);
        byte[] objectBytes = out.getData();
        String enumSetBase64 = Base64.encodeBase64String(objectBytes);

        opArguments.put("enumSetBase64", enumSetBase64);
        opArguments.put("createParent", createParent);
        LOG.warn("Using hard-coded replication value of 1.");
        opArguments.put("replication", 1);
        opArguments.put("blockSize", blockSize);

        // Include a flag to indicate whether or not the policy is non-null.
        opArguments.put("policyExists", policy != null);

        // Only include these if the policy is non-null.
        if (policy != null) {
            opArguments.put("codec", policy.getCodec());
            opArguments.put("targetReplication", policy.getTargetReplication());
        }

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "create",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation create to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation create to NameNode.");
        }

        // Extract the result from the Json response.
        // If there's an exception, then it will be logged by this function.
        Object result = null;
        try {
            result = this.serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (result != null)
            stat = (HdfsFileStatus)result;

        return stat;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions)
            throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException,
            FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException,
            SafeModeException, UnresolvedLinkException, IOException {
        return this.create(src, masked, clientName, flag, createParent, replication, blockSize, supportedVersions, null);
    }

    @Override
    public LastBlockWithStatus append(String src, String clientName, EnumSetWritable<CreateFlag> flag) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        LastBlockWithStatus stat = null;

        // Arguments for the 'append' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        // Serialize the `EnumSetWritable<CreateFlag> flag` argument.
        DataOutputBuffer out = new DataOutputBuffer();
        ObjectWritable.writeObject(out, flag, flag.getClass(), null);
        byte[] objectBytes = out.getData();
        String enumSetBase64 = Base64.encodeBase64String(objectBytes);

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("flag", enumSetBase64);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "append",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation append to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation append to NameNode.");
        }

        // Extract the result from the Json response.
        // If there's an exception, then it will be logged by this function.
        Object result = null;
        try {
            result = this.serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (result != null)
            stat = (LastBlockWithStatus)result;

        return stat;
    }

    @Override
    public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(byte storagePolicyID) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return new BlockStoragePolicy[0];
    }

    @Override
    public void setStoragePolicy(String src, String policyName) throws UnresolvedLinkException, FileNotFoundException, QuotaExceededException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setMetaStatus(String src, MetaStatus metaStatus) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("metaStatus", metaStatus.ordinal());

        try {
            submitOperationToNameNode(
                    "setMetaStatus",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setMetaStatus to NameNode:", ex);
        }
    }

    @Override
    public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("permission", permission);

        try {
            submitOperationToNameNode(
                    "setPermission",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setPermission to NameNode:", ex);
        }
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("username", username);
        opArguments.put("groupname", groupname);

        try {
            submitOperationToNameNode(
                    "setOwner",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setOwner to NameNode:", ex);
        }
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("holder", holder);
        opArguments.put("fileId", fileId);
        opArguments.put("b", b);

        try {
            submitOperationToNameNode(
                    "abandonBlock",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation abandonBlock to NameNode:", ex);
        }
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes,
                                 long fileId, String[] favoredNodes) throws IOException, ClassNotFoundException {
        // HashMap<String, Object> opArguments = new HashMap<>();
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("previous", previous);
        opArguments.put("fileId", fileId);
        opArguments.put("favoredNodes", favoredNodes);
        opArguments.put("excludeNodes", excludeNodes);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "addBlock",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addBlock to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addBlock to NameNode.");
        }

        Object result = serverlessInvoker.extractResultFromJsonResponse(responseJson);

        if (result != null) {
            LocatedBlock locatedBlock = (LocatedBlock) result;

            LOG.debug("Result returned from addBlock() is of type: " + result.getClass().getSimpleName());
            LOG.debug("LocatedBlock returned by addBlock(): " + locatedBlock);

            return locatedBlock;
        }

        return null;
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src, long fileId, ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId, byte[] data) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("last", last);
        opArguments.put("fileId", fileId);
        opArguments.put("data", data);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "complete",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation complete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation complete to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (boolean)result;

        return true;
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public boolean rename(String src, String dst) throws UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[1];

        optionsArr[0] = 0; // 0 is the Options.Rename ordinal/value for `NONE`

        opArguments.put("options", optionsArr);

        try {
            submitOperationToNameNode(
                    "rename",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation rename to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation rename to NameNode.");
        }

        return true;
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("trg", trg);
        opArguments.put("srcs", srcs);

        try {
            submitOperationToNameNode(
                    "concat",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation concat to NameNode:", ex);
        }
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[options.length];

        for (int i = 0; i < options.length; i++) {
            optionsArr[i] = options[i].ordinal();
        }

        opArguments.put("options", optionsArr);

        try {
            submitOperationToNameNode(
                    "rename", // Not rename2, we just map 'rename' to 'renameTo' or 'rename2' or whatever
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation rename to NameNode:", ex);
        }
    }

    @Override
    public boolean truncate(String src, long newLength, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("newLength", newLength);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "truncate",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation truncate to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation truncate to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (boolean)result;

        return true;
    }

    @Override
    public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("recursive", recursive);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "delete",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation delete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation delete to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (boolean)result;

        return false;
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("masked", masked);
        opArguments.put("createParent", createParent);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "mkdirs",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation mkdirs to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation mkdirs to NameNode.");
        }

        boolean result = false;
        try {
            result = (boolean) serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("startAfter", startAfter);
        opArguments.put("needLocation", needLocation);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "getListing",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getListing to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getListing to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (DirectoryListing)result;

        return null;
    }

    @Override
    public void renewLease(String clientName) throws AccessControlException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);

        try {
            submitOperationToNameNode(
                    "renewLease",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation renewLease to NameNode:", ex);
        }
    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        return false;
    }

    @Override
    public long[] getStats() throws IOException {
        return new long[0];
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type) throws IOException {
        return new DatanodeInfo[0];
    }

    @Override
    public DatanodeStorageReport[] getDatanodeStorageReport(HdfsConstants.DatanodeReportType type) throws IOException {
        return new DatanodeStorageReport[0];
    }

    @Override
    public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {
        return 0;
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        return false;
    }

    @Override
    public void refreshNodes() throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "getFileInfo",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getFileInfo to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getFileInfo to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (HdfsFileStatus)result;

        return null;
    }

    @Override
    public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "isFileClosed",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation isFileClosed to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation isFileClosed to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
            throw new IOException("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
        }
        if (result != null)
            return (boolean)result;

        return false;
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        JsonObject responseJson;
        try {
            responseJson = submitOperationToNameNode(
                    "getFileLinkInfo",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getFileLinkInfo to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation isFileClosed to NameNode.");
        }

        Object result = null;
        try {
            result = serverlessInvoker.extractResultFromJsonResponse(responseJson);
        } catch (ClassNotFoundException e) {
            LOG.error("Exception encountered whilst extracting result of `complete()` " +
                    "operation from JSON response.");
            e.printStackTrace();
        }
        if (result != null)
            return (HdfsFileStatus)result;

        return null;
    }

    @Override
    public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setQuota(String path, long namespaceQuota, long storagespaceQuota, StorageType type) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        
    }

    @Override
    public void fsync(String src, long inodeId, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorages) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        return 0;
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void ping() throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public SortedActiveNodeList getActiveNamenodesForClient() throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void changeConf(List<String> props, List<String> newVals) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public EncodingStatus getEncodingStatus(String filePath) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void encodeFile(String filePath, EncodingPolicy policy) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void revokeEncoding(String filePath, short replication) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock getRepairedBlockLocations(String sourcePath, String parityPath, LocatedBlock block, boolean isParity) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void checkAccess(String path, FsAction mode) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LastUpdatedContentSummary getLastUpdatedContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeDefaultAcl(String src) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeAcl(String src) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public AclStatus getAclStatus(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void createEncryptionZone(String src, String keyName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public EncryptionZone getEZForPath(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> listXAttrs(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeXAttr(String src, XAttr xAttr) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        return 0;
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId, CacheDirectiveInfo filter) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void modifyCachePool(CachePoolInfo req) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeCachePool(String pool) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addUser(String userName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);

        try {
            submitOperationToNameNode(
                    "addUser",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addUser to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addUser to NameNode.");
        }
    }

    @Override
    public void addGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "addGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addGroup to NameNode.");
        }
    }

    @Override
    public void addUserToGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "addUserToGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addUserToGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addUserToGroup to NameNode.");
        }
    }

    @Override
    public void removeUser(String userName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);

        try {
            submitOperationToNameNode(
                    "removeUser",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeUser to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeUser to NameNode.");
        }
    }

    @Override
    public void removeGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "removeGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeGroup to NameNode.");
        }
    }

    @Override
    public void removeUserFromGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "removeUserFromGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeUserFromGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeUserFromGroup to NameNode.");
        }
    }

    @Override
    public void invCachesUserRemoved(String userName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesGroupRemoved(String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserRemovedFromGroup(String userName, String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserAddedToGroup(String userName, String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long getEpochMS() throws IOException {
        return 0;
    }
}
