package org.apache.hadoop.hdfs.server.namenode;

import com.google.gson.JsonObject;
import io.hops.exception.StorageInitializtionException;
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
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.serverless.ArgumentContainer;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvoker;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

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
    public ServerlessInvoker<JsonObject> serverlessInvoker;

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

    public ServerlessNameNodeClient(Configuration conf, DFSClient dfsClient) throws StorageInitializtionException {
        // "https://127.0.0.1:443/api/v1/web/whisk.system/default/namenode?blocking=true";
        serverlessEndpointBase = conf.get(SERVERLESS_ENDPOINT, SERVERLESS_ENDPOINT_DEFAULT);
        serverlessPlatformName = conf.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT);

        LOG.info("Serverless endpoint: " + serverlessEndpointBase);
        LOG.info("Serverless platform: " + serverlessPlatformName);

        this.serverlessInvoker = dfsClient.serverlessInvoker;

        this.dfsClient = dfsClient;
    }


    @Override
    public JsonObject latencyBenchmark(String connectionUrl, String dataSource, String query, int id) throws SQLException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        LocatedBlocks locatedBlocks = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("offset", offset);
        opArguments.put("length", length);

        JsonObject responseJson = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "getBlockLocations",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        // We do not have any additional/non-default arguments to pass to the NN.
        JsonObject responseJson = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "getServerDefaults",
                dfsClient.serverlessEndpoint, new HashMap<>(), new HashMap<>());

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
            throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException,
            FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException,
            SafeModeException, UnresolvedLinkException, IOException {
        // We need to pass a series of arguments to the Serverless NameNode. We prepare these arguments here
        // in a HashMap and pass them off to the ServerlessInvoker, which will package them up in the required
        // format for the Serverless NameNode.
        HdfsFileStatus stat = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("masked", masked.toShort());
        opArguments.put("clientName", dfsClient.clientName);

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

        JsonObject responseJson = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "create",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("clientName", clientName);
        opArguments.put("flag", enumSetBase64);

        JsonObject responseJson = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "append",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("metaStatus", metaStatus.ordinal());

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "setMetaStatus",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("permission", permission);

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "setPermission",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("username", username);
        opArguments.put("groupname", groupname);

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "setOwner",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes,
                                 long fileId, String[] favoredNodes) throws IOException, ClassNotFoundException {
        // HashMap<String, Object> opArguments = new HashMap<>();
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("clientName", clientName);
        opArguments.put("previous", previous);
        opArguments.put("fileId", fileId);
        opArguments.put("favoredNodes", favoredNodes);
        opArguments.put("excludeNodes", excludeNodes);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "addBlock",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("clientName", clientName);
        opArguments.put("last", last);
        opArguments.put("fileId", fileId);
        opArguments.put("data", data);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "complete",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[1];

        optionsArr[0] = 0; // 0 is the Options.Rename ordinal/value for `NONE`

        opArguments.put("options", optionsArr);

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "rename",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

        return true;
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("trg", trg);
        opArguments.put("srcs", srcs);

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "concat",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[options.length];

        for (int i = 0; i < options.length; i++) {
            optionsArr[i] = options[i].ordinal();
        }

        opArguments.put("options", optionsArr);

        serverlessInvoker.invokeNameNodeViaHttpPost(
                "rename",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public boolean truncate(String src, long newLength, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("src", src);
        opArguments.put("newLength", newLength);
        opArguments.put("clientName", clientName);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "truncate",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("recursive", recursive);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "delete",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("masked", masked);
        opArguments.put("createParent", createParent);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "mkdirs",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);
        opArguments.put("startAfter", startAfter);
        opArguments.put("needLocation", needLocation);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "getListing",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("clientName", clientName);

        serverlessInvoker.invokeNameNodeViaHttpPost(
            "renewLease",
            serverlessEndpointBase,
            null, // We do not have any additional/non-default arguments to pass to the NN.
            opArguments);
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

        opArguments.put("src", src);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "getFileInfo",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "isFileClosed",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        opArguments.put("src", src);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "getFileLinkInfo",
                serverlessEndpointBase,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);

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

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "addUser",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void addGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "addGroup",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void addUserToGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "addUserToGroup",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void removeUser(String userName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "removeUser",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void removeGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "removeGroup",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
    }

    @Override
    public void removeUserFromGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "removeUserFromGroup",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments);
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
