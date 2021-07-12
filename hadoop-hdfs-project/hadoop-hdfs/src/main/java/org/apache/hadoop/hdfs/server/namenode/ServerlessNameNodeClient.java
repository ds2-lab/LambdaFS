package org.apache.hadoop.hdfs.server.namenode;

import com.google.gson.JsonObject;
import io.hops.exception.StorageInitializtionException;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.MetaStatus;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.NotImplementedException;
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
import org.apache.hadoop.hdfs.serverless.ServerlessInvoker;
import org.apache.hadoop.hdfs.serverless.ServerlessInvokerFactory;
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
     */
    public String serverlessEndpoint;

    /**
     * The name of the serverless platform being used for the Serverless NameNodes.
     */
    public String serverlessPlatformName;

    private DFSClient dfsClient;

    public ServerlessNameNodeClient(Configuration conf, DFSClient dfsClient) throws StorageInitializtionException {
        // "https://127.0.0.1:443/api/v1/web/whisk.system/default/namenode?blocking=true"; //
        serverlessEndpoint = conf.get(SERVERLESS_ENDPOINT, SERVERLESS_ENDPOINT_DEFAULT);
        serverlessPlatformName = conf.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT);

        LOG.info("Serverless endpoint: " + serverlessEndpoint);
        LOG.info("Serverless platform: " + serverlessPlatformName);

        this.serverlessInvoker = ServerlessInvokerFactory.getServerlessInvoker(serverlessPlatformName);

        this.dfsClient = dfsClient;
    }


    @Override
    public JsonObject latencyBenchmark(String connectionUrl, String dataSource, String query, int id) throws SQLException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlocks getMissingBlockLocations(String filePath) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void addBlockChecksum(String src, int blockIndex, long checksum) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public long getBlockChecksum(String src, int blockIndex) throws IOException {
        return 0;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        FsServerDefaults serverDefaults = null;

        JsonObject responseJson = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "getServerDefaults",
                dfsClient.serverlessEndpoint.toString(),
                null, // We do not have any additional/non-default arguments to pass to the NN.
                null);

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
        HashMap<String, Object> opArguments = new HashMap<>();

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
        opArguments.put("replication", replication);
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
                dfsClient.serverlessEndpoint.toString(),
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
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(byte storagePolicyID) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return new BlockStoragePolicy[0];
    }

    @Override
    public void setStoragePolicy(String src, String policyName) throws UnresolvedLinkException, FileNotFoundException, QuotaExceededException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setMetaStatus(String src, MetaStatus metaStatus) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes) throws AccessControlException, FileNotFoundException, NotReplicatedYetException, SafeModeException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src, long fileId, ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId, byte[] data) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        HashMap<String, Object> opArguments = new HashMap<>();

        opArguments.put("src", src);
        opArguments.put("clientName", clientName);
        opArguments.put("last", last);
        opArguments.put("fileId", fileId);
        opArguments.put("data", data);

        JsonObject responseJson = serverlessInvoker.invokeNameNodeViaHttpPost(
                "complete",
                serverlessEndpoint.toString(),
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
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public boolean rename(String src, String dst) throws UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException, UnresolvedLinkException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws AccessControlException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public boolean truncate(String src, long newLength, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void renewLease(String clientName) throws AccessControlException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
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
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setQuota(String path, long namespaceQuota, long storagespaceQuota, StorageType type) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        
    }

    @Override
    public void fsync(String src, long inodeId, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorages) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        return 0;
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void ping() throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public SortedActiveNodeList getActiveNamenodesForClient() throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void changeConf(List<String> props, List<String> newVals) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public EncodingStatus getEncodingStatus(String filePath) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void encodeFile(String filePath, EncodingPolicy policy) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void revokeEncoding(String filePath, short replication) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock getRepairedBlockLocations(String sourcePath, String parityPath, LocatedBlock block, boolean isParity) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void checkAccess(String path, FsAction mode) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public LastUpdatedContentSummary getLastUpdatedContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeDefaultAcl(String src) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeAcl(String src) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public AclStatus getAclStatus(String src) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void createEncryptionZone(String src, String keyName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public EncryptionZone getEZForPath(String src) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> listXAttrs(String src) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeXAttr(String src, XAttr xAttr) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        return 0;
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId, CacheDirectiveInfo filter) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void modifyCachePool(CachePoolInfo req) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeCachePool(String pool) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool) throws IOException {
        throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void addUser(String userName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void addGroup(String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void addUserToGroup(String userName, String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeUser(String userName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeGroup(String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void removeUserFromGroup(String userName, String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserRemoved(String userName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesGroupRemoved(String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserRemovedFromGroup(String userName, String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserAddedToGroup(String userName, String groupName) throws IOException {
		throw new NotImplementedException("Function has not yet been implemented.");
    }

    @Override
    public long getEpochMS() throws IOException {
        return 0;
    }
}
