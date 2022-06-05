/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.gson.JsonObject;
import org.apache.hadoop.hdfs.serverless.execution.ExecutionManager;
import org.apache.hadoop.util.VersionInfo;
import io.hops.DalDriver;
import io.hops.events.EventManager;
import io.hops.exception.StorageException;
import io.hops.leaderElection.LeaderElection;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.*;
import io.hops.metadata.hdfs.entity.DatanodeStorage;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.metrics.TransactionEvent;
import io.hops.security.HopsUGException;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.serverless.invoking.InvokerUtilities;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerBase;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerFactory;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNodeList;
import org.apache.hadoop.hdfs.serverless.execution.taskarguments.TaskArguments;
import org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.userserver.NameNodeTcpUdpClient;
import org.apache.hadoop.hdfs.serverless.zookeeper.SyncZKClient;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.ssl.RevocationListFetcherService;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tracing.TraceAdminProtocol;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.hash.Hashing.consistentHash;
import static io.hops.transaction.lock.LockFactory.getInstance;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.TracerConfigurationManager;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_LENGTH;
import static org.apache.hadoop.hdfs.serverless.BaseHandler.localModeEnabled;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;
import static org.apache.hadoop.hdfs.serverless.invoking.ServerlessUtilities.extractParentPath;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import org.apache.htrace.core.Tracer;

/**
 * ********************************************************
 * NameNode serves as both directory namespace manager and "inode table" for
 * the
 * Hadoop DFS. There is a single NameNode running in any DFS deployment. (Well,
 * except when there is a second backup/failover NameNode, or when using
 * federated NameNodes.)
 * <p/>
 * The NameNode controls two critical tables: 1) filename->blocksequence
 * (namespace) 2) block->machinelist ("inodes")
 * <p/>
 * The first table is stored on disk and is very precious. The second table is
 * rebuilt every time the NameNode comes up.
 * <p/>
 * 'NameNode' refers to both this class as well as the 'NameNode server'. The
 * 'FSNamesystem' class actually performs most of the filesystem management.
 * The
 * majority of the 'NameNode' class itself is concerned with exposing the IPC
 * interface and the HTTP server to the outside world, plus some configuration
 * management.
 * <p/>
 * NameNode implements the
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} interface, which
 * allows clients to ask for DFS services.
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} is not designed for
 * direct use by authors of DFS client code. End-users should instead use the
 * {@link org.apache.hadoop.fs.FileSystem} class.
 * <p/>
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol} interface,
 * used by DataNodes that actually store DFS data blocks. These methods are
 * invoked repeatedly and automatically by all the DataNodes in a DFS
 * deployment.
 * <p/>
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol} interface,
 * used by secondary namenodes or rebalancing processes to get partial NameNode
 * state, for example partial blocksMap etc.
 * ********************************************************
 */
@InterfaceAudience.Private
public class ServerlessNameNode implements NameNodeStatusMXBean {

  static {
    HdfsConfiguration.init();
  }

  /**
   * Determines whether we use an instance of {@link NameNodeResult}
   * or {@link org.apache.hadoop.hdfs.serverless.execution.results.NameNodeResultWithMetrics}.
   */
  public static ThreadLocal<Boolean> benchmarkingModeEnabled = ThreadLocal.withInitial(() -> false);

  /**
   * The singleton ServerlessNameNode instance associated with this container. There can only be one!
   */
  private static ServerlessNameNode instance;

  /**
   * Used to listen for events from NDB.
   */
  private EventManager ndbEventManager;

  /**
   * Manages the execution of file system operations.
   */
  private ExecutionManager executionManager;

  /**
   * Thread in which the EventManager runs.
   */
  private Thread eventManagerThread;

  /**
   * How often the DataNodes are supposed to publish heartbeats/storage reports to intermediate storage.
   * The units of this variable are milliseconds.
   */
  private long heartBeatInterval;

  /**
   * If true, use NDB as intermediate storage during consistency protocol (i.e., to host ACKs and INVs).
   * Otherwise, use ZooKeeper.
   */
  private boolean useNdbForConsistencyProtocol;

  /**
   * Added by Ben; mostly used for debugging (i.e., making sure the NameNode code that
   * is running is up-to-date with the source code base).
   *
   * Syntax:
   *  Major.Minor.Build.Revision
   */
  public static final String versionNumber = "0.4.0.0";

  /**
   * The number of uniquely-deployed serverless name nodes associated with this particular Serverless HopsFS cluster.
   *
   * This is used when hashing parent inode IDs to particular serverless name nodes.
   */
  private int numDeployments;

  /**
   * List of currently-active NameNodes. This list is based on metadata stored in NDB.
   *
   * The list is updated in one of two ways:
   *  (1) The worker thread periodically refreshes the list when it has no other work to do.
   *  (2) The list is updated when the NameNode is first created.
   */
  private ActiveServerlessNameNodeList activeNameNodes;

  /**
   * A mapping from operation/function name to the respective functions. We use this to call FS operations and whatever
   * other functions as directed by clients and DataNodes.
   */
  private Map<String, CheckedFunction<TaskArguments, ? extends Serializable>> operations;

  /**
   * HashSet containing the names of all write operations.
   * Used to check if a given operation is a write operation or not.
   */
  public static final HashSet<String> WRITE_OPERATIONS;

  /**
   * When the 'op' field is set to this in the invocation payload, no operation is performed.
   */
  private static final String DEFAULT_OPERATION = "default";

  /**
   * ZooKeeper client. Used to track membership.
   */
  private ZKClient zooKeeperClient;

  /**
   * Used by the Serverless NameNode to invoke other Serverless NameNodes.
   */
  private ServerlessInvokerBase serverlessInvoker;

  /**
   * The base endpoint to which we direct requests when invoking serverless functions.
   */
  private String serverlessEndpointBase;

  /**
   * The time at which this instance of the NameNode began executing.
   *
   * This is used to initially grab StorageReport instances from NDB. In regular HopsFS, NameNodes
   * would only begin receiving StorageReports once everything is running. Old StorageReports would
   * obviously have just not been sent. So we only query for StorageReports that were published beginning
   * with the time that the NameNode began executing.
   */
  private final long creationTime;

  /**
   * Connection to the ZooKeeper cluster/server.
   */
  private ZooKeeper zooKeeper;

  /**
   * Zookeeper framework-style client
   */
  private CuratorFramework curatorFramework;

  /**
   * The last time an intermediate storage updated occurred.
   *
   * Initialized to -1 so that an update occurs the first time it is attempted.
   */
  private volatile long lastIntermediateStorageUpdate = -1L;

  /**
   * When enabled, we "spoof" network operations to NDB. This is expected to be done exclusively
   * with file read operations. We basically just return a hard-coded {@link LocatedBlocks} object.
   * This is used for testing our performance without NDB network operations being on the critical path.
   */
  private boolean spoofNdbOperations = false;

  /**
   * HDFS configuration can have three types of parameters:
   * <ol>
   * <li>Parameters that are common for all the name services in the
   * cluster.</li>
   * <li>Parameters that are specific to a name service. These keys are
   * suffixed with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * <li>Parameters that are specific to a single name node. These keys are
   * suffixed with nameserviceId and namenodeId in the configuration. for
   * example, "dfs.namenode.rpc-address.nameservice1.namenode1"</li>
   * </ol>
   * <p/>
   * In the latter cases, operators may specify the configuration without any
   * suffix, with a nameservice suffix, or with a nameservice and namenode
   * suffix. The more specific suffix will take precedence.
   * <p/>
   * These keys are specific to a given namenode, and thus may be configured
   * globally, for a nameservice, or for a specific namenode within a
   * nameservice.
   */
  public static final String[] NAMENODE_SPECIFIC_KEYS = {DFS_NAMENODE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY, DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_HTTPS_ADDRESS_KEY,
    DFS_NAMENODE_KEYTAB_FILE_KEY,
    DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY};

  private static final String USAGE =
      "Usage: java NameNode [" +
          //StartupOption.BACKUP.getName() + "] | [" +
          //StartupOption.CHECKPOINT.getName() + "] | [" +
          StartupOption.FORMAT.getName() + " [" +
          StartupOption.CLUSTERID.getName() + " cid ] | [" +
          StartupOption.FORCE.getName() + "] [" +
          StartupOption.NONINTERACTIVE.getName() + "] ] | [" +
          //StartupOption.UPGRADE.getName() + "] | [" +
          //StartupOption.ROLLBACK.getName() + "] | [" +
          StartupOption.ROLLINGUPGRADE.getName() + " "
          + RollingUpgradeStartupOption.getAllOptionString() + " ] | \n   [" +
          //StartupOption.FINALIZE.getName() + "] | [" +
          //StartupOption.IMPORT.getName() + "] | [" +
          //StartupOption.INITIALIZESHAREDEDITS.getName() + "] | [" +
          //StartupOption.BOOTSTRAPSTANDBY.getName() + "] | [" +
          //StartupOption.RECOVER.getName() + " [ " +
          //StartupOption.FORCE.getName() + " ] ] | [ "+
          StartupOption.NO_OF_CONCURRENT_BLOCK_REPORTS.getName() + " concurrentBlockReports ] | [" +
          StartupOption.FORMAT_ALL.getName() + " ]";

  static {
    WRITE_OPERATIONS = Sets.newHashSet(
            "abandonBlock", "addBlock", "append", "complete", "concat", "create", "delete",
            "mkdirs", "rename", "setOwner", "setPermission", "setMetaStatus", "truncate"
    );
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID;
    } else if (protocol.equals(DatanodeProtocol.class.getName())) {
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())) {
      return NamenodeProtocol.versionID;
    } else if (protocol
        .equals(RefreshAuthorizationPolicyProtocol.class.getName())) {
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())) {
      return RefreshUserMappingsProtocol.versionID;
    } else if (protocol.equals(RefreshCallQueueProtocol.class.getName())) {
      return RefreshCallQueueProtocol.versionID;
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())){
      return GetUserMappingsProtocol.versionID;
    } else if (protocol.equals(TraceAdminProtocol.class.getName())){
      return TraceAdminProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  public static final int DEFAULT_PORT = 8020;
  public static final Logger LOG = LoggerFactory.getLogger(ServerlessNameNode.class.getName());
  //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
  public static final Logger stateChangeLog = LoggerFactory.getLogger("org.apache.hadoop.hdfs.StateChange");
  public static final Logger blockStateChangeLog = LoggerFactory.getLogger("BlockStateChange");

  private static final String NAMENODE_HTRACE_PREFIX = "namenode.htrace.";

  protected FSNamesystem namesystem;
  protected final Configuration conf;
  private AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Used to record transaction events that have occurred while processing the current request.
   * Cleared after each request is processed.
   */
  private final ThreadLocal<Set<TransactionEvent>> transactionEvents = new ThreadLocal<>();

  /**
   * Identifies the NameNode. This is used in place of the leader election ID since leader election is not used
   * by serverless name nodes.
   *
   * This is set the first time this function is invoked (when it is warm, it should still be set...).
   *
   * The value is computed by hashing the activation ID of the OpenWhisk function that created the instance.
   */
  protected long nameNodeID = -1L;

  /**
   * This variable is used to keep track of the last storage report retrieved from intermediate storage.
   */
  private final HashMap<String, Long> lastStorageReportGroupIds = new HashMap<>();

  /**
   * Used to keep track of the most recent reportId obtained for each data node.
   */
  private final HashMap<String, Long> lastIntermediateBlockReportTimestamp = new HashMap<>();

  /**
   * The name of the serverless function in which this NameNode instance is running.
   */
  private final String functionName;

  /**
   * The number of this serverless function.
   */
  private final int deploymentNumber;

  /**
   * The amount of RAM (in megabytes) that this function has been allocated. Used when determining the number of active
   * TCP connections that this NameNode can have at once, as each TCP connection has two relatively-large buffers. If
   * the NN creates too many TCP connections at once, then it might crash due to OOM errors.
   */
  private final int actionMemory;

  /**
   * Used to communicate with Serverless HopsFS clients via TCP.
   */
  private NameNodeTcpUdpClient nameNodeTCPClient;

  /**
   * How long we should wait for ACKs during a transaction in which we're serving as the Leader.
   *
   * At some point, we need to stop waiting so reads/writes aren't blocked indefinitely.
   *
   * Units: milliseconds.
   */
  private int txAckTimeout;

  /**
   * Determines if the given request (identified by its request ID) has already been received and processed by the NN.
   *
   * @param requestId The ID of the request.
   * @return true if the request has been received and processed already, otherwise false.
   */
  public boolean checkIfRequestProcessedAlready(String requestId) {
    return executionManager.isTaskDuplicate(requestId);
  }

  public ExecutionManager getExecutionManager() { return executionManager; }

  public int getNumDeployments() {
    return numDeployments;
  }

  public NameNodeTcpUdpClient getNameNodeTcpClient() {
    return nameNodeTCPClient;
  }

  public void addTransactionEvent(TransactionEvent event) {
    Set<TransactionEvent> localTxEvents = transactionEvents.get();

    if (localTxEvents == null) {
      localTxEvents = new HashSet<>();
      transactionEvents.set(localTxEvents);
    }

    localTxEvents.add(event);
  }

  public void addTransactionEvents(Collection<TransactionEvent> events) {
    Set<TransactionEvent> localTxEvents = transactionEvents.get();

    if (localTxEvents == null) {
      localTxEvents = new HashSet<>();
      transactionEvents.set(localTxEvents);
    }

    localTxEvents.addAll(events);
  }

  /**
   * Return a COPY of the transactionEvents list.
   */
  public List<TransactionEvent> getTransactionEvents() {
    Set<TransactionEvent> localTxEvents = transactionEvents.get();

    if (localTxEvents == null) {
      localTxEvents = new HashSet<>();
      transactionEvents.set(localTxEvents);
    }

    return new ArrayList<>(localTxEvents);
  }

  public List<TransactionEvent> getAndClearTransactionEvents() {
    Set<TransactionEvent> localTxEvents = transactionEvents.get();

    if (localTxEvents == null) {
      localTxEvents = new HashSet<>();
      transactionEvents.set(localTxEvents);
    } else {
      // If they were previously null, then we don't need to call clear.
      localTxEvents.clear();
    }

    return new ArrayList<>(localTxEvents);
  }

  public void clearTransactionEvents() {
    Set<TransactionEvent> localTxEvents = transactionEvents.get();

    if (localTxEvents != null)
      localTxEvents.clear();
  }

  /**
   * Retrieve and process the various updates that are stored in intermediate storage.
   *
   * These updates include:
   *  - Registering new DataNodes;
   *  - Processing storage reports from DataNodes;
   *  - Processing intermediate block reports from DataNodes.
   */
  public void getAndProcessUpdatesFromIntermediateStorage() throws IOException, ClassNotFoundException {
    registerDataNodesFromIntermediateStorage();

    List<DatanodeDescriptor> dataNodes =
            namesystem.getBlockManager()
                      .getDatanodeManager()
                      .getDatanodeListForReport(HdfsConstants.DatanodeReportType.ALL);

    retrieveAndProcessStorageReports(dataNodes);
    getAndProcessIntermediateBlockReports();

    lastIntermediateStorageUpdate = System.currentTimeMillis();
  }

  /**
   * Create a transaction to read the INode for the specified file from NDB.
   * @return
   */
  public INode getINodeForCache(final String srcArg) throws IOException {
    final FSPermissionChecker pc = namesystem.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = namesystem.dir.resolvePath(pc, srcArg, pathComponents);
    final boolean isSuperUser =  namesystem.dir.getPermissionChecker().isSuperUser();

    HopsTransactionalRequestHandler getINodeRequestHandler = new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_BLOCK_LOCATIONS, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(
                TransactionLockTypes.INodeLockType.READ,
                        TransactionLockTypes.INodeResolveType.PATH, src)
                .setNameNodeID(getId())
                .setActiveNameNodes(getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock()).add(lf.getBlockRelated(
                LockFactory.BLK.RE, LockFactory.BLK.ER, LockFactory.BLK.CR,
                LockFactory.BLK.UC, LockFactory.BLK.CA));
        locks.add(lf.getEZLock());
        if (isSuperUser) {
          locks.add(lf.getXAttrLock());
        } else {
          locks.add(lf.getXAttrLock(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO));
        }
      }

      @Override
      public Object performTask() throws IOException {
        return namesystem.getINode(src);
      }
    };

    return (INode) getINodeRequestHandler.handle();
  }

  /**
   * Check if the in-memory INode cache should be updated during the execution of this operation.
   *
   * We partition the namespace by parent INode ID (or possibly the fully-qualified path of the parent INode).
   * We cache all metadata entries up to the terminal entry on the path.
   * As such, the metadata cache should only be write-enabled if we're performing an operation on a file or directory
   * that is mapped to our deployment.
   *
   * This function will enable or disable metadata cache updates/writes. That is, it will make a call to
   * {@link EntityManager#toggleMetadataCacheWrites}.
   *
   * @param target The fully-qualified path of the target file or directory.
   */
  private void toggleMetadataCacheWritesForCurrentOp(String target) {
    if (target == null) {
      if (LOG.isTraceEnabled()) LOG.trace("Received NULL for target path. Disabling metadata cache updates by default.");
      EntityManager.toggleMetadataCacheWrites(false);
      return;
    }

    // Step 1: Extract the parent path.
    String parentOfTargetPath = extractParentPath(target);

    // Compute the target deployment number via consistent hashing of the parent directory of `target`.
    int targetDeployment = (consistentHash(Hashing.md5().hashString(parentOfTargetPath), numDeployments));

    boolean enableUpdates = (targetDeployment == deploymentNumber);

    if (LOG.isTraceEnabled())
      LOG.debug("Target path '" + target + "' has parent '" + parentOfTargetPath +
              "'. Parent consistently hashed to deployment " + targetDeployment +
              ". Cache updates will be " + (enableUpdates ? "ENABLED." : "DISABLED."));

    // Enable or disable metadata cache writes based on whether the target deployment num matches our deployment num.
    EntityManager.toggleMetadataCacheWrites(enableUpdates);
  }

  /**
   * Returns the singleton ServerlessNameNode instance.
   *
   * @param commandLineArguments The command-line arguments given to the NameNode during initialization.
   * @param functionName The name of this particular OpenWhisk action.
   * @param actionMemory The amount of RAM (in megabytes) that this function has been allocated. Used when
   *                     determining the number of active TCP connections that this NameNode can have at once, as
   *                     each TCP connection has two relatively-large buffers. If the NN creates too many TCP
   *                     connections at once, then it might crash due to OOM errors.
   * @param isCold Indicates whether this is a cold start.
   */
  public static synchronized ServerlessNameNode getOrCreateNameNodeInstance(String[] commandLineArguments,
                                                                            String functionName, int actionMemory,
                                                                            boolean isCold)
          throws Exception {
    if (instance != null) {
      if (LOG.isDebugEnabled()) LOG.debug("Using existing NameNode instance with ID = " + instance.getId());
      return instance;
    }

    if (!isCold)
      LOG.warn("Container is warm, but there is no existing ServerlessNameNode instance.");

    instance = ServerlessNameNode.startServerlessNameNode(commandLineArguments, functionName, actionMemory);
    instance.populateOperationsMap();

    // Next, the NameNode needs to exit safe mode (if it is in safe mode).
    if (instance.isInSafeMode()) {
      instance.getNamesystem().leaveSafeMode();
    }

    return instance;
  }

  /**
   * Attempt to get the singleton ServerlessNameNode instance. This function will throw an exception if the
   * instance does not exist. This is useful for trying to get the instance when you expect/need it to exist.
   * This should be used when the caller feels that the ServerlessNameNode instance SHOULD exist, and that it would
   * be an error if it did not exist when this function is called.
   * @param exceptOnFailure If true, throw an exception if the NameNode does not exist.
   * @return The ServerlessNameNode instance, if it exists.
   * @throws IllegalStateException Thrown if the ServerlessNameNode instance does not exist and `exceptOnFailure`
   * is true.
   */
  public static synchronized ServerlessNameNode tryGetNameNodeInstance(boolean exceptOnFailure)
          throws IllegalStateException {
    if (instance != null)
      return instance;

    if (exceptOnFailure)
      throw new IllegalStateException("ServerlessNameNode instance does not exist!");
    else
      return null;
  }

  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }

  public int getTxAckTimeout() {
    return txAckTimeout;
  }

  /**
   * Wrapper interface, so we can embed all the operation-functions in a HashMap for easy calling.
   *
   * Sources:
   *  - https://stackoverflow.com/questions/18198176/java-8-lambda-function-that-throws-exception
   *  - https://stackoverflow.com/questions/4480334/how-to-call-a-method-stored-in-a-hashmap-java
   */
  @FunctionalInterface
  public interface CheckedFunction<T, R extends Serializable> {
    R apply(T arg) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException;
  }

  /**
   * Check if the given operation is a write operation.
   * @param op The name of the operation.
   * @return True if the operation is a write operation, otherwise false.
   */
  public static boolean isWriteOperation(String op) {
    return WRITE_OPERATIONS.contains(op);
  }

  /**
   * Populate the operations HashMap with all the functions.
   * Each supported FS operation has a function mapped to the operation's name.
   */
  public void populateOperationsMap() {
    operations = new HashMap<>();

    operations.put("abandonBlock", args -> {
      abandonBlock(args);
      return null;
    });
    operations.put("addBlock", this::addBlock);
    operations.put("addGroup", args -> {
      addGroup(args);
      return null;
    });
    operations.put("addUser", args -> {
      addUser(args);
      return null;
    });
    operations.put("addUserToGroup", args -> {
      addUserToGroup(args);
      return null;
    });
    operations.put("append", this::append);
    operations.put("complete", this::complete);
    operations.put("concat", args -> {
      concat(args);
      return null;
    });
    operations.put("create", this::create);
    operations.put("delete", this::delete);
    operations.put("getActiveNamenodesForClient", args -> (Serializable)getActiveNameNodesWithRefresh());
    operations.put("getBlockLocations", this::getBlockLocations);
    operations.put("getDatanodeReport", this::getDatanodeReport);
    operations.put("getFileInfo", this::getFileInfo);
    operations.put("getFileLinkInfo", this::getFileLinkInfo);
    operations.put("getListing", this::getListing);
    operations.put("getServerDefaults", this::getServerDefaults);
    operations.put("getStats", this::getStats);
    operations.put("isFileClosed", this::isFileClosed);
    operations.put("mkdirs", this::mkdirs);
    operations.put("ping", args ->  {
      return null;
    });
    operations.put("prewarm", args ->  {
      try {
        Thread.sleep(500);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      return null;
    });
    operations.put("removeUser", args -> {
      removeUser(args);
      return null;
    });
    operations.put("removeGroup", args -> {
      removeGroup(args);
      return null;
    });
    operations.put("removeUserFromGroup", args -> {
      removeUserFromGroup(args);
      return null;
    });
    operations.put("rename", args -> {
      rename(args);
      return null;
    });
    operations.put("renewLease", args -> {
      renewLease(args);
      return null;
    });
    operations.put("setMetaStatus", args -> {
      setMetaStatus(args);
      return null;
    });
    operations.put("setOwner", args -> {
      setOwner(args);
      return null;
    });
    operations.put("setPermission", args -> {
      setPermission(args);
      return null;
    });
    operations.put("subtreeDeleteSubOperation", this::subtreeDeleteSubOperation);
    operations.put("truncate", this::truncate);
    operations.put("updatePipeline", args -> {
      updatePipeline(args);
      return null;
    });
    operations.put("updateBlockForPipeline", this::updateBlockForPipeline);
    operations.put("versionRequest", this::versionRequest);
  }

  /**
   * Perform the operation specified by the String (which will contain the operations name). Pass the arguments
   * contained in fsArgs to the function.
   *
   * @param fsArgs The arguments to be passed to the desired FS operation.
   * @param op The name of the desired FS operation to be performed.
   */
  public Serializable performOperation(String op, TaskArguments fsArgs)
          throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    //LOG.info("Specified operation: " + op);

    if (op == null || op.equals(DEFAULT_OPERATION)) {
      LOG.info("User did not specify an operation (or specified the default operation " + DEFAULT_OPERATION + ").");
      return null;
    }

    // Attempt to extract the source argument.
    // If it exists, then we'll enable or disable metadata cache writes accordingly.
    toggleMetadataCacheWritesForCurrentOp(fsArgs.getString(SRC));

    return this.operations.get(op).apply(fsArgs);
  }

  public void refreshActiveNameNodesList() throws IOException {
    synchronized (this) {
      if (activeNameNodes == null)
        activeNameNodes = new ActiveServerlessNameNodeList(this.zooKeeperClient, this.numDeployments);
    }

    activeNameNodes.refreshFromZooKeeper(this.zooKeeperClient);
  }

  /**
   * Used as the entry-point into the Serverless NameNode when executing a serverless function.
   *
   * @param commandLineArgs Command-line arguments formatted as if the NameNode was being executed from the commandline.
   * @param actionMemory The amount of RAM (in megabytes) that this function has been allocated. Used when
   *                     determining the number of active TCP connections that this NameNode can have at once, as
   *                     each TCP connection has two relatively-large buffers. If the NN creates too many TCP
   *                     connections at once, then it might crash due to OOM errors.
   * @param functionName The name of this serverless function.
   * @throws Exception
   */
  public static ServerlessNameNode startServerlessNameNode(String[] commandLineArgs, String functionName,
                                                           int actionMemory)
          throws Exception {
    if (DFSUtil.parseHelpArgument(commandLineArgs, ServerlessNameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    LOG.info("Creating and initializing new instance of Serverless NameNode now...");

    try {
      ServerlessNameNode nameNode = createNameNode(commandLineArgs, null, functionName, actionMemory, true);

      if (nameNode == null) {
        LOG.info("ERROR: NameNode is null. Failed to create and/or initialize the Serverless NameNode.");
        terminate(1);
      } else {
        LOG.info("Successfully created and initialized Serverless NameNode.");
      }

      return nameNode;
    } catch (Throwable e) {
      LOG.error("Failed to start namenode.", e);
      terminate(1, e);
    }

    return null;
  }

  /**
   * Retrieve the DatanodeStorage instances stored in intermediate storage.
   * These are used in conjunction with StorageReports.
   *
   * This method will convert the objects from their DAL versions to the HopsFS versions.
   */
  private HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> retrieveAndConvertDatanodeStorages(
          DatanodeDescriptor datanodeDescriptor) throws IOException {
//    LOG.info("Retrieving DatanodeStorage instances from intermediate storage now...");

    DatanodeStorageDataAccess<DatanodeStorage> dataAccess =
            (DatanodeStorageDataAccess)HdfsStorageFactory.getDataAccess(DatanodeStorageDataAccess.class);

    List<DatanodeStorage> datanodeStorages = dataAccess.getDatanodeStorages(datanodeDescriptor.getDatanodeUuid());

    HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> convertedDatanodeStorageMap
            = new HashMap<>();

    for (DatanodeStorage datanodeStorage : datanodeStorages) {
      org.apache.hadoop.hdfs.server.protocol.DatanodeStorage convertedDatanodeStorage =
              new org.apache.hadoop.hdfs.server.protocol.DatanodeStorage(datanodeStorage.getStorageId(),
                      org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State.values()[datanodeStorage.getState()],
                      StorageType.values()[datanodeStorage.getStorageType()]);

      convertedDatanodeStorageMap.put(convertedDatanodeStorage.getStorageID(), convertedDatanodeStorage);
    }

    return convertedDatanodeStorageMap;
  }

  /**
   * Retrieve the StorageReports from intermediate storage. The NameNode maintains
   * the most recent groupId for each DataNode, and we use this reference to ensure
   * we are retrieving the latest StorageReports.
   */
  private HashMap<String, List<io.hops.metadata.hdfs.entity.StorageReport>> retrieveStorageReports(
          List<DatanodeDescriptor> datanodeDescriptors) throws IOException {
//    LOG.info("Retrieving StorageReport instances for " + datanodeDescriptors.size() + " data nodes now...");

    HashMap<String, List<io.hops.metadata.hdfs.entity.StorageReport>> storageReportMap = new HashMap<>();

    for (DatanodeDescriptor datanodeDescriptor : datanodeDescriptors) {
      List<io.hops.metadata.hdfs.entity.StorageReport> storageReports = retrieveStorageReports(datanodeDescriptor);
      storageReportMap.put(datanodeDescriptor.getDatanodeUuid(), storageReports);
    }

    return storageReportMap;
  }

  /**
   * Retrieve the storage reports associated with one particular DataNode.
   *
   * @param datanodeDescriptor
   * @return List of converted StorageReport instances (converted from DAL representation to internal HopsFS representation)
   * @throws IOException
   */
  private List<io.hops.metadata.hdfs.entity.StorageReport> retrieveStorageReports(DatanodeDescriptor datanodeDescriptor)
          throws IOException {
    StorageReportDataAccess<io.hops.metadata.hdfs.entity.StorageReport> dataAccess =
            (StorageReportDataAccess)HdfsStorageFactory.getDataAccess(StorageReportDataAccess.class);

    String datanodeUuid = datanodeDescriptor.getDatanodeUuid();
    // The groupId column for Storage Reports is actually being used as a timestamp now. So we default to a little
    // before the time that the NN was created, as any reports published before that point would not have been received
    // by a traditional, serverful HopsFS NN anyway. We opt for times a little early just so we can bootstrap the
    // storage of the DN, since the NN may be performing an operation rn that requires us to know about the DN's
    // storage. So we grab some old reports, so we know about the storages that it has.
    long lastStorageReportGroupId =
            lastStorageReportGroupIds.getOrDefault(datanodeUuid,creationTime - (heartBeatInterval * 3));

    // Commented this out because:
    // Now, I just block intermediate-storage-updates from occurring more often than every heartbeatInterval.
    // This is because the worker thread performs the updates now. So the worker thread just checks for updates
    // every heartbeat interval.

    long now = System.currentTimeMillis();

    List<io.hops.metadata.hdfs.entity.StorageReport> storageReports
        = dataAccess.getStorageReportsAfterGroupId(lastStorageReportGroupId, datanodeUuid);

    // Sometimes there is a "race" where the DN wrote the storage report per its heartbeat interval, but we don't
    // see it yet. Not sure why it happens. But we don't update the timestamp unless we actually read a storage report.
    // That way, we'll just get the report the next time we query for reports.
    if (storageReports.size() > 0)
      // Update the entry for this DN, as we just retrieved its Storage Reports.
      lastStorageReportGroupIds.put(datanodeUuid, now);

    return storageReports;
  }

  /**
   * Get the StorageReport and DatanodeStorage instances from intermediate storage and perform the necessary
   * processing in order to populate the NameNode with the relevant information.
   *
   * @return The largest groupId processes during this operation.
   */
  private void retrieveAndProcessStorageReports(List<DatanodeDescriptor> datanodes) throws IOException {
    // Procedure:
    // 1) Retrieve StorageReport instances
    // 2) Retrieve DatanodeStorage instances.
    // 3) Convert these objects from the DAL versions to the HopsFS versions.
    // 4) Pass them off to the handler method in FSFilesystem.

    // Create a mapping from each DataNode UUID to the map containing all of its converted DatanodeStorage instances.
    // This is simply a map of maps. The keys of the outer map are datanode UUIDs. The values are HashMaps.
    // The keys of the inner map are storageIds. The values of the inner map are DatanodeStorage instances.
    HashMap<String, HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage>> datanodeStorageMaps
            = new HashMap<>();

    // Iterate over each DatanodeRegistration, representing a registered DataNode. Retrieve its DatanodeStorage
    // instances from intermediate storage. Create a mapping of them & store the mapping in the HashMap defined above.
    for (DatanodeDescriptor registration : datanodes) {
      String datanodeUuid = registration.getDatanodeUuid();

//      LOG.info("Retrieving DatanodeRegistration instances for datanode " + datanodeUuid);
      HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> datanodeStorageMap
              = this.retrieveAndConvertDatanodeStorages(registration);

      datanodeStorageMaps.put(datanodeUuid, datanodeStorageMap);
    }

    // Map to keep track of the largest groupId retrieved for each DataNode. Specifically, this maps
    // the DataNode's UUID to the largest groupId retrieved during this operation.
    HashMap<String, Long> largestGroupIds = new HashMap<>();

    // Retrieve the storage reports from intermediate storage.
    HashMap<String, List<io.hops.metadata.hdfs.entity.StorageReport>> storageReportMap
            = this.retrieveStorageReports(datanodes);

    HashMap<String, List<org.apache.hadoop.hdfs.server.protocol.StorageReport>> convertedStorageReportMap
            = new HashMap<>();

    // Iterate over all of the storage reports. The keys are datanodeUuids and the values are storage reports.
    for (Map.Entry<String, List<StorageReport>> entry : storageReportMap.entrySet()) {
      String datanodeUuid = entry.getKey();
      List<StorageReport> storageReports = entry.getValue();

      if (storageReports == null) {
//        LOG.warn("StorageReport list for DataNode " + datanodeUuid + " is null. Skipping.");
        convertedStorageReportMap.put(datanodeUuid, null);
        continue;
      }

      // Get the mapping of storageIds to DatanodeStorage instances for this particular datanode.
      HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> datanodeStorageMap
              = datanodeStorageMaps.get(datanodeUuid);

      ArrayList<org.apache.hadoop.hdfs.server.protocol.StorageReport> convertedStorageReports =
              new ArrayList<>();

      // For each storage report associated with the current datanode, convert it to a HopsFS storage report (they
      // are currently the DAL storage reports, which are just designed to be used with intermediate storage).
      for (StorageReport report : storageReports) {

        String datanodeStorageId = report.getDatanodeStorageId();
        org.apache.hadoop.hdfs.server.protocol.DatanodeStorage datanodeStorage =
                datanodeStorageMap.get(datanodeStorageId);

        if (datanodeStorage != null) {
          org.apache.hadoop.hdfs.server.protocol.StorageReport convertedReport
                  = new org.apache.hadoop.hdfs.server.protocol.StorageReport(
                  datanodeStorageMap.get(report.getDatanodeStorageId()), report.getFailed(),
                  report.getCapacity(), report.getDfsUsed(), report.getRemaining(), report.getBlockPoolUsed());

          convertedStorageReports.add(convertedReport);

          if (report.getGroupId() > largestGroupIds.getOrDefault(report.getDatanodeUuid(), -1L))
            largestGroupIds.put(report.getDatanodeUuid(), report.getGroupId());
        }
        else {
          LOG.warn("StorageReport (id=" + report.getReportId() + ", group="
                  + report.getGroupId() + ") from DataNode " + datanodeUuid + " is referencing an unknown datanode " +
                  "storage (id=" + datanodeStorageId + "). Skipping this report.");
        }
      }

      convertedStorageReportMap.put(datanodeUuid, convertedStorageReports);
    }

    int numSuccess = 0;               // If we have zero successes and zero skips, that's a problem.
    int numSkippedIntentionally = 0;  // Keep track of how many we've skipped on purpose.
                                      // Having no successes AND intentional skips is an okay scenario.

    // For each registration, we call the `handleServerlessStorageReports()` function. We pass the given
    // registration, then we retrieve the list of storage reports from the mapping, convert it to an Object[],
    // and cast it to an array of HopsFS StorageReport[].
    for (DatanodeDescriptor datanodeDescriptor : datanodes) {
      try {
        // Grab the list of converted storage reports.
        List<org.apache.hadoop.hdfs.server.protocol.StorageReport> convertedStorageReports =
                convertedStorageReportMap.get(datanodeDescriptor.getDatanodeUuid());

        // Check if it is null. If it is null, then there are intentionally no storage reports for this DN.
        // If there were "unintentionally" no storage reports, then it would be of length zero. When it is null,
        // it means we grabbed the last batch of storage reports within the last heartbeat interval, so there will
        // not be any new ones. If we pass an array of length zero to `handleServerlessReports()`, the NN will
        // unregister the storages of the DN bc it thinks the DN stopped reporting about them, indicating that
        // they're gone. So we use a null value to say "no, it's fine, we know there aren't any reports yet."
        if (convertedStorageReports == null) {
          LOG.warn("There are no converted storage reports associated with DataNode "
                  + datanodeDescriptor.getDatanodeUuid() + ". Skipping...");
          numSkippedIntentionally++;
          continue;
        }

        this.namesystem.handleServerlessStorageReports(datanodeDescriptor,
                convertedStorageReports.toArray(org.apache.hadoop.hdfs.server.protocol.StorageReport.EMPTY_ARRAY));

        numSuccess++; // We successfully processed those reports, so increment this counter.
      } catch (IOException ex) {
        // We catch this exception so that the entire NameNode doesn't fail if just one of the DataNodes fails
        // to register. There can also be issues where an old DataNode crashed completely and wasn't able to remove
        // its metadata from intermediate storage before exiting.

        // TODO: Keep track of failures to register DN entries in intermediate storage. If enough NNs fail to
        //       register a particular DataNode, we could just consider than DN to have failed, and one of the NNs
        //       could remove the metadata from intermediate storage, and possibly update a separate table to indicate
        //       that the DN failed. That DataNode, if it is in-fact running, could watch for NNs reporting its own
        //       failure, and then try to resolve the issue and eventually rewrite its metadata to intermediate storage.
        LOG.warn("Failed to handle storage reports for DataNode " + datanodeDescriptor.getDatanodeUuid() + ": ", ex);
      }
    }

    // We don't need to consider datanode registrations that we intentionally skipped in this upcoming
    // check. There's only a problem if there were non-skipped registrations and we successfully
    // processed NONE of them.
    int numNotSkipped = datanodes.size() - numSkippedIntentionally;

    // If we didn't register ANY DataNodes, then we should raise an exception here,
    // as we won't have any DataNodes with which to complete file system operations.
    if (numNotSkipped > 0 && numSuccess == 0) {
      // TODO: What if we already have some valid DNs registered? We shouldn't throw an exception in this case.
      //       We should add an additional condition that we also do not have any valid DNs.
      throw new IOException("Failed to successfully process any of the " + datanodes.size() +
              " non-skipped registration(s).");
    }
//    else if (datanodes.size() == 0) {
//      LOG.warn("There were NO datanode registrations to process...");
//    }
  }

  /**
   * Retrieve Intermediate Block Reports from intermediate storage and process them.
   */
  private void getAndProcessIntermediateBlockReports() throws IOException, ClassNotFoundException {
    IntermediateBlockReportDataAccess<IntermediateBlockReport> dataAccess =
            (IntermediateBlockReportDataAccess) HdfsStorageFactory.getDataAccess(IntermediateBlockReportDataAccess.class);

//    LOG.info("Retrieving intermediate block reports from intermediate storage now...");
//    LOG.info("There are " + lastIntermediateBlockReportTimestamp.size()
//            + " DataNodes for which reports must be retrieved.");

    // Keep track of the DNs whose intermediate block reports we process.
    // We'll update the timestamp after this loop.
    long now = System.currentTimeMillis();
    ArrayList<String> processedReports = new ArrayList<>();
    for (Map.Entry<String, Long> entry : lastIntermediateBlockReportTimestamp.entrySet()) {
      String datanodeUuid = entry.getKey();
      long lastTimestamp = entry.getValue();

//      LOG.info("Retrieving all reports with timestamp >= " + (lastTimestamp + 1) + " for DataNode "
//              + datanodeUuid + " now...");

      List<IntermediateBlockReport> reports = dataAccess.getReportsPublishedAfter(datanodeUuid, lastTimestamp);

//      LOG.info("Retrieved " + reports.size() + " intermediate block reports published by DataNode "
//              + datanodeUuid + ".");

      for (IntermediateBlockReport report : reports) {
        StorageReceivedDeletedBlocks[] blocksArr =
                (StorageReceivedDeletedBlocks[])InvokerUtilities.base64StringToObject(report.getReceivedAndDeletedBlocks());

//        LOG.info("Processing " + blocksArr.length + " StorageReceivedDeletedBlocks instances from intermediate " +
//                "block report " + report.getReportId() + ", DataNode UUID = " + datanodeUuid + " now...");

        for (StorageReceivedDeletedBlocks blocks : blocksArr) {
          namesystem.processIncrementalBlockReport(report.getDatanodeUuid(), blocks);
        }
      }

      processedReports.add(datanodeUuid);
    }

    // Update the entries for all the DNs whose reports we processed.
    for (String datanodeUuid: processedReports)
      lastIntermediateBlockReportTimestamp.put(datanodeUuid, now);
  }

  /**
   * Retrieve the DataNodes from intermediate storage. Register any that are not already registered.
   *
   * TODO: Unregister any DataNodes that are not in intermediate storage. Like, if we have a DN registered, but
   *       its metadata is no longer found in intermediate storage, we should probably remove/unregister it...
   *
   * @return List of DatanodeRegistration instances to be used to retrieve serverless storage reports once
   * the registration step(s) have been completed.
   */
  private List<DatanodeRegistration> registerDataNodesFromIntermediateStorage() throws IOException {
    // Retrieve the DataNodes from intermediate storage.
//    LOG.info("Retrieving list of DataNodes from intermediate storage now...");
    DataNodeDataAccess<DataNodeMeta> dataAccess = (DataNodeDataAccess)
            HdfsStorageFactory.getDataAccess(DataNodeDataAccess.class);
    List<DataNodeMeta> dataNodes = dataAccess.getAllDataNodes();

    // LOG.info("Retrieved list of DataNodes from intermediate storage with " + dataNodes.size() + " entries!");

    NamespaceInfo nsInfo = namesystem.getNamespaceInfo();

    // Keep track of the DatanodeRegistration instances because we'll need these to retrieve
    // the storage reports from intermediate storage after we've registered the data node(s).
    List<DatanodeRegistration> datanodeRegistrations = new ArrayList<>();

    // Iterate over all the DataNodes retrieved from intermediate storage. We need to do some pre-processing here
    // to avoid errors during the registration process. If we have two DataNodeMeta objects with the same IP address
    // and port, then they are referring to the same DataNode. DataNodes are uniquely identified by their IP, port,
    // and UUID. But we only care if the IP and port are the same. In this case, we only keep the most-recent DN based
    // on the 'creation_time' field.
    HashMap<String, DataNodeMeta> dnMap = new HashMap<>();
    int numReplaced = 0;
    for (DataNodeMeta dataNodeMeta : dataNodes) {
      String key = dataNodeMeta.getIpAddress() + ":" + dataNodeMeta.getXferPort();

      // If the map already contains the key, then we check if the DataNodeMeta in the map is older or newer than
      // the one from the for-each loop. If the for-each loop instance is newer, then we replace the one in the map
      // with the one from the for-each loop. If the for-each loop instance is older, then we skip the for-each loop
      // instance, as we want the newest DN associated with a given ip address + port.
      if (dnMap.containsKey(key))
      {
        // It just exists in this mapping; it hasn't already been registered necessarily.
        // But we don't want to bother trying to register this DN if it's just old metadata
        // from a DN that no longer exists/that has been replaced by a newer DN.
        DataNodeMeta existingDN = dnMap.get(key);
        int creationTimeComparisonResult = dataNodeMeta.compareCreationTimes(existingDN);

        // If the DN from the for-loop has a creation time stamp larger than the DN already in the map, then we replace
        // the DN in the map with the instance from the for-loop. If the creation times are the same, or if the DN from
        // the for-loop has a smaller creation time stamp, then we do nothing and move onto the next DN in the for-loop.
        if (creationTimeComparisonResult > 0) {
          dnMap.put(key, dataNodeMeta);
          numReplaced++;
        }
      }
      else {
        dnMap.put(key, dataNodeMeta);
      }
    }
    // TODO: Need to remove old DataNode metadata from intermediate storage. Apparently stop-dfs.sh doesn't
    //       allow DataNodes to shutdown cleanly, meaning they aren't cleaning up their metadata upon exiting.

    for (DataNodeMeta dataNodeMeta : dnMap.values()) {
      String datanodeUuid = dataNodeMeta.getDatanodeUuid();

      DatanodeID dnId =
          new DatanodeID(dataNodeMeta.getIpAddress(), dataNodeMeta.getHostname(),
                  datanodeUuid, dataNodeMeta.getXferPort(), dataNodeMeta.getInfoPort(),
                  dataNodeMeta.getInfoSecurePort(), dataNodeMeta.getIpcPort(), dataNodeMeta.getCreationTime());

      StorageInfo storageInfo = new StorageInfo(
              DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
              nsInfo.getNamespaceID(), nsInfo.clusterID, nsInfo.getCTime(),
              HdfsServerConstants.NodeType.DATA_NODE, nsInfo.getBlockPoolID());

      DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
              dnId, storageInfo, new ExportedBlockKeys(), VersionInfo.getVersion());

      // Create an entry for this DataNode.
      if (!lastIntermediateBlockReportTimestamp.containsKey(datanodeUuid)) {
        lastIntermediateBlockReportTimestamp.put(datanodeUuid, creationTime);
      }

      if (namesystem.getBlockManager().getDatanodeManager().getDatanodeByUuid(
              datanodeRegistration.getDatanodeUuid()) != null) {
        continue;
      } else {
        namesystem.registerDatanode(datanodeRegistration);
      }

      datanodeRegistrations.add(datanodeRegistration);
    }

    return datanodeRegistrations;
  }

  private LocatedBlock addBlock(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String clientName = fsArgs.getString("clientName"); // fsArgs.getAsJsonPrimitive("clientName").getAsString();

    long fileId = fsArgs.getLong("fileId"); // fsArgs.getAsJsonPrimitive("fileId").getAsLong();

    String[] favoredNodes = fsArgs.getStringArray("favoredNodes");

//    if (fsArgs.contains("favoredNodes")) {
//      JsonArray favoredNodesJsonArray = fsArgs.getAsJsonArray("favoredNodes");
//      favoredNodes = new String[favoredNodesJsonArray.size()];
//
//      for (int i = 0; i < favoredNodesJsonArray.size(); i++) {
//        favoredNodes[i] = favoredNodesJsonArray.get(i).getAsString();
//      }
//    }

    ExtendedBlock previous = fsArgs.getObject("previous");

//    if (fsArgs.contains("previous")) {
////      String previousBase64 = fsArgs.getAsJsonPrimitive("previous").getAsString();
////      previous = (ExtendedBlock) InvokerUtilities.base64StringToObject(previousBase64);
//
//      previous = fsArgs.getObject("previous");;
//    }

    DatanodeInfo[] excludeNodes = fsArgs.getObjectArray("excludedNodes");
//    if (fsArgs.contains("excludeNodes")) {
//      // Decode and deserialize the DatanodeInfo[].
////      JsonArray excludedNodesJsonArray = fsArgs.getAsJsonArray("excludeNodes");
////      excludeNodes = new DatanodeInfo[excludedNodesJsonArray.size()];
////
////      for (int i = 0; i < excludedNodesJsonArray.size(); i++) {
////        String excludedNodeBase64 = excludedNodesJsonArray.get(i).getAsString();
////        DatanodeInfo excludedNode = (DatanodeInfo) InvokerUtilities.base64StringToObject(excludedNodeBase64);
////        excludeNodes[i] = excludedNode;
////      }
//
//    }

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
              "*BLOCK* NameNode.addBlock: file " + src + " fileId=" + fileId + " for " + clientName);
    }
    HashSet<Node> excludedNodesSet = null;

    if (excludeNodes != null) {
      excludedNodesSet = new HashSet<>(excludeNodes.length);
      excludedNodesSet.addAll((Collection<? extends Node>) Arrays.asList(excludeNodes));
    }
    List<String> favoredNodesList = null;
    if (favoredNodes != null)
      favoredNodesList = Arrays.asList(favoredNodes);

    return namesystem.getAdditionalBlock(src, fileId, clientName, previous, excludedNodesSet, favoredNodesList);
  }

  private void addUser(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    String userName = fsArgs.getString("userName"); // fsArgs.getAsJsonPrimitive("userName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.addUser(userName);
  }

  private void addGroup(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    String groupName = fsArgs.getString("groupName"); // fsArgs.getAsJsonPrimitive("groupName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.addGroup(groupName);
  }

  private void addUserToGroup(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    String userName = fsArgs.getString("userName"); // fsArgs.getAsJsonPrimitive("userName").getAsString();
    String groupName = fsArgs.getString("groupName"); // fsArgs.getAsJsonPrimitive("groupName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.addUserToGroup(userName, groupName);
  }

  public void removeUser(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String userName = fsArgs.getString("userName"); // fsArgs.getAsJsonPrimitive("userName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.removeUser(userName);
  }

  public void removeGroup(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String groupName = fsArgs.getString("groupName"); // fsArgs.getAsJsonPrimitive("groupName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.removeGroup(groupName);
  }

  public void removeUserFromGroup(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String userName = fsArgs.getString("userName"); // fsArgs.getAsJsonPrimitive("userName").getAsString();
    String groupName = fsArgs.getString("groupName"); // fsArgs.getAsJsonPrimitive("groupName").getAsString();

    namesystem.checkSuperuserPrivilege();
    UsersGroups.removeUserFromGroup(userName, groupName);
  }

  private LastBlockWithStatus append(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    String src = fsArgs.getString(SRC); // fsArgs.getAsJsonPrimitive(ServerlessNameNodeKeys.SRC).getAsString();
    String clientName = fsArgs.getString(CLIENT_NAME); // fsArgs.getAsJsonPrimitive(CLIENT_NAME).getAsString();

//    byte[] enumSetSerialized = Base64.decodeBase64(fsArgs.getAsJsonPrimitive(FLAG).getAsString());
//
//    DataInputBuffer dataInput = new DataInputBuffer();
//    dataInput.reset(enumSetSerialized, enumSetSerialized.length);
    EnumSetWritable<CreateFlag> flag = fsArgs.getObject(FLAG);// (EnumSetWritable<CreateFlag>) ObjectWritable.readObject(dataInput, null));

    String clientMachine = NameNodeRpcServer.getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
              +src+" for "+clientName+" at "+clientMachine);
    }
    LastBlockWithStatus info = namesystem.appendFile(src, clientName, clientMachine, flag.get());
    metrics.incrFilesAppended();
    return info;
  }

  private boolean complete(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String clientName = fsArgs.getString("clientName"); // fsArgs.getAsJsonPrimitive("clientName").getAsString();

    long fileId = fsArgs.getLong("fileId"); // fsArgs.getAsJsonPrimitive("fileId").getAsLong();

    ExtendedBlock last = fsArgs.getObject("last");

    // TODO: Add helper/utility functions to reduce boilerplate code when extracting arguments.
    //       References:
    //       - https://stackoverflow.com/questions/11664894/jackson-deserialize-using-generic-class
    //       - https://stackoverflow.com/questions/11659844/jackson-deserialize-generic-class-variable
    //       - https://stackoverflow.com/questions/17400850/is-jackson-really-unable-to-deserialize-json-into-a-generic-type
//    if (fsArgs.contains("last")) {
//      String lastBase64 = fsArgs.getAsJsonPrimitive("last").getAsString();
//      last = (ExtendedBlock) InvokerUtilities.base64StringToObject(lastBase64);
//    }

    byte[] data = fsArgs.getObject("data");

//    if (fsArgs.contains("data")) {
//      String dataBase64 = fsArgs.getAsJsonPrimitive("data").getAsString();
//      data = Base64.decodeBase64(dataBase64);
//    }

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: " + src + " fileId=" + fileId +" for " + clientName);
    }

    // Check to see if we've been sent an intermediate block report.
    getAndProcessIntermediateBlockReports();

    return namesystem.completeFile(src, clientName, last, fileId, data);
  }

  private DirectoryListing getListing(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    //LOG.info("Unpacking arguments for the GET-LISTING operation now...");

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    boolean needLocation = fsArgs.getBoolean("needLocation"); // fsArgs.getAsJsonPrimitive("needLocation").getAsBoolean();

//    String startAfterBase64 = fsArgs.getString("startAfter"); // fsArgs.getAsJsonPrimitive("startAfter").getAsString();
//    byte[] startAfter = Base64.decodeBase64(startAfterBase64);

    byte[] startAfter = fsArgs.getByteArray("startAfter");

    DirectoryListing files =
            namesystem.getListing(src, startAfter, needLocation);
    if (files != null) {
      metrics.incrGetListingOps();
      metrics.incrFilesInGetListingOps(files.getPartialListing().length);
    }
    return files;
  }

  private void setMetaStatus(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();

    int metaStatusOrdinal = fsArgs.getInt("metaStatus"); // fsArgs.getAsJsonPrimitive("metaStatus").getAsInt();
    MetaStatus metaStatus = MetaStatus.values()[metaStatusOrdinal];

    namesystem.setMetaStatus(src, metaStatus);
  }

  private void setPermission(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
//    String permissionBase64 = fsArgs.get("permission"); // fsArgs.getAsJsonArray("permission").getAsString();
//    FsPermission permission = (FsPermission) InvokerUtilities.base64StringToObject(permissionBase64);
    FsPermission permission = fsArgs.getObject("permission");

    namesystem.setPermission(src, permission);
  }

  private void setOwner(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String username = fsArgs.getString("userName"); // fsArgs.getAsJsonPrimitive("userName").getAsString();
    String groupname = fsArgs.getString("groupName"); // fsArgs.getAsJsonPrimitive("groupName").getAsString();

    namesystem.setOwner(src, username, groupname);
  }

  private boolean mkdirs(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    //String maskedBase64 = fsArgs.getString("masked"); // fsArgs.getAsJsonPrimitive("masked").getAsString();
    //FsPermission masked = (FsPermission) InvokerUtilities.base64StringToObject(maskedBase64);
    FsPermission masked = fsArgs.getObject("masked");
    boolean createParent = fsArgs.getBoolean("createParent"); // fsArgs.getAsJsonPrimitive("createParent").getAsBoolean();

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException(
              "mkdirs: Pathname too long.  Limit " + MAX_PATH_LENGTH +
              "mkdirs: Pathname too long.  Limit " + MAX_PATH_LENGTH +
                      " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src, new PermissionStatus(
            getRemoteUser().getShortUserName(), null,
            masked), createParent);
  }

  private boolean isFileClosed(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    //LOG.info("Unpacking arguments for the IS-FILE-CLOSED operation now...");
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();

    return namesystem.isFileClosed(src);
  }

  private LocatedBlocks getBlockLocations(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    //LOG.info("Unpacking arguments for the GET-BLOCK-LOCATIONS operation now...");

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    long offset = fsArgs.getLong("offset"); // fsArgs.getAsJsonPrimitive("offset").getAsLong();
    long length = fsArgs.getLong("length"); // fsArgs.getAsJsonPrimitive("length").getAsLong();

    metrics.incrGetBlockLocations();

    if (spoofNdbOperations) {
      LOG.debug("Spoofing getBlockLocations() operation for src argument '" + src + "'");
      return new LocatedBlocks(0, false, new ArrayList<>(),
              null, false, null);
    }

    return namesystem.getBlockLocations(NameNodeRpcServer.getClientMachine(), src, offset, length);
  }

  private void abandonBlock(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String holder = fsArgs.getString("holder"); // fsArgs.getAsJsonPrimitive("holder").getAsString();
    long fileId = fsArgs.getLong("fileId"); // fsArgs.getAsJsonPrimitive("fileId").getAsLong();

    ExtendedBlock b = null;

    if (fsArgs.contains("b")) {
      String previousBase64 = fsArgs.getString("b"); // fsArgs.getAsJsonPrimitive("b").getAsString();
      b = (ExtendedBlock) InvokerUtilities.base64StringToObject(previousBase64);
    }

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog
              .debug("*BLOCK* NameNode.abandonBlock: " + b + " of file " + src);
    }
    if (!namesystem.abandonBlock(b, fileId, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  private void concat(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    //LOG.info("Unpacking arguments for the CONCAT operation now...");

    String trg = fsArgs.getString("trg"); // fsArgs.getAsJsonPrimitive("trg").getAsString();

//    JsonArray srcsArr = fsArgs.getAsJsonPrimitive("srcsArr").getAsJsonArray();
//    String[] srcs = new String[srcsArr.size()];
//
//    for (int i = 0; i < srcs.length; i++) {
//      String src = srcsArr.get(i).getAsString();
//      srcs[i] = src;
//    }

    String[] srcs = fsArgs.getStringArray("srcsArr");

    namesystem.concat(trg, srcs);
  }

  private HdfsFileStatus getFileInfo(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();

    return namesystem.getFileInfo(src, true);
  }

  private HdfsFileStatus getFileLinkInfo(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();

    return namesystem.getFileInfo(src, false);
  }

  /**
   * This function is only ever issued/invoked by other NameNodes. During subtree delete operations, the Leader
   * NameNode will split up the task of deleting all the files in the subtree into many sub-operations that are
   * executed in-parallel. Normally this all occurs on a single NN. Because Serverless NNs do not have a large
   * amount of vCPU allocated to them, it is better to spread these sub-operations out among multiple Serverless
   * NNs. To do this, the Leader NN will issue requests to other NameNodes, requesting that they execute this
   * function in order to help process the delete operation.
   *
   * @param args The arguments passed by the Leader NN.
   *
   * @return True if the delete operation was successful, otherwise false.
   */
  private boolean subtreeDeleteSubOperation(TaskArguments args)
          throws IOException {
    EntityManager.toggleMetadataCacheReads(false);

    // The INode ID of the INode corresponding to the subtree root.
    long subtreeRootId = args.getLong("subtreeRootId"); // args.getAsJsonPrimitive("subtreeRootId").getAsLong();

    // The unique ID of the leader NameNode orchestrating this subtree operation.
    long leaderNameNodeID = args.getLong("leaderNameNodeID"); // args.getAsJsonPrimitive("leaderNameNodeID").getAsLong();

    // The full paths of the files/directories that we should delete.
//    JsonArray pathsJson = args.getAsJsonArray("pathsJson");
//    List<String> paths = new ArrayList<>();
//    for (int i = 0; i < pathsJson.size(); i++) {
//      paths.add(pathsJson.get(i).getAsString());
//    }
    List<String> paths = args.getStringList("paths");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Executed batched subtree operation. Leader NN ID: " + leaderNameNodeID + ".");
      LOG.debug("Subtree root path: '" + subtreeRootId + "', subtree root ID: " + subtreeRootId + ".");
      LOG.debug("There are " + paths.size() + " paths to delete: " + StringUtils.join(paths, ", "));
    }
    ArrayList<Future> barrier = new ArrayList<>();
    for (String path : paths) {
      if (LOG.isDebugEnabled()) LOG.debug("Submitting deletion for path '" + path + "' now...");
      Future f = FSDirDeleteOp.multiTransactionDeleteInternal(namesystem, path, subtreeRootId);
      barrier.add(f);
    }

    boolean success = FSDirDeleteOp.processResponses(barrier);

    if (!success) {
      LOG.error("Batched subtree delete op on subtree " + subtreeRootId + " FAILED.");
      return false;
    }

    if (LOG.isDebugEnabled()) LOG.debug("Batched subtree delete op on subtree " + subtreeRootId + " succeeded.");
    return true;
  }

  private NamespaceInfo versionRequest(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);

    String datanodeUuid = fsArgs.getString("uuid"); // fsArgs.get("uuid").getAsString();

    namesystem.checkSuperuserPrivilege();
    NamespaceInfo nsInfo = namesystem.getNamespaceInfo();

    // TODO: Does this still make sense?
    // Check for an existing groupId associated with this DataNode.
    // This would exist if the DN had crashed and is restarting or something to that effect.
    long groupId = this.lastStorageReportGroupIds.getOrDefault(datanodeUuid, creationTime);
    if (LOG.isDebugEnabled()) LOG.debug("Assigning groupId " + groupId + " to DN " + datanodeUuid);
    nsInfo.setGroupId(groupId);

    return nsInfo;
  }

  /**
   (String src, FsPermission masked,
   String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
   short replication, long blockSize, CryptoProtocolVersion[] supportedVersions, EncodingPolicy policy)
   */

  private HdfsFileStatus create(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    //LOG.info("Unpacking arguments for the CREATE operation now...");

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    short permissionAsShort = fsArgs.getShort("masked"); // fsArgs.getAsJsonPrimitive("masked").getAsShort();
    FsPermission masked = new FsPermission(permissionAsShort);
    String clientName = fsArgs.getString("clientName"); // fsArgs.getAsJsonPrimitive("clientName").getAsString();

    //byte[] enumSetSerialized = Base64.decodeBase64(fsArgs.getAsJsonPrimitive("enumSetBase64").getAsString());
    byte[] enumSetSerialized = Base64.decodeBase64(fsArgs.getString("enumSetBase64"));

    DataInputBuffer dataInput = new DataInputBuffer();
    dataInput.reset(enumSetSerialized, enumSetSerialized.length);
    EnumSet<CreateFlag> flag = ((EnumSetWritable<CreateFlag>) ObjectWritable.readObject(dataInput, null)).get();

    boolean createParent = fsArgs.getBoolean("createParent"); // fsArgs.getAsJsonPrimitive("createParent").getAsBoolean();
    short replication = fsArgs.getShort("replication"); // fsArgs.getAsJsonPrimitive("replication").getAsShort();
    long blockSize = fsArgs.getLong("blockSize"); // fsArgs.getAsJsonPrimitive("blockSize").getAsLong();
    CryptoProtocolVersion[] supportedVersions = CryptoProtocolVersion.supported();

    EncodingPolicy policy = null;
    boolean policyExists = fsArgs.getBoolean("policyExists"); // fsArgs.getAsJsonPrimitive("policyExists").getAsBoolean();
    if (policyExists) {
      String codec = fsArgs.getString("codec"); // fsArgs.getAsJsonPrimitive("codec").getAsString();
      short targetReplication = fsArgs.getShort("targetReplication"); // fsArgs.getAsJsonPrimitive("targetReplication").getAsShort();
      policy = new EncodingPolicy(codec, targetReplication);
    }

    LOG.info("Create Arguments:\n   src = " + src + "\n   clientName = "+ clientName + "\n   createParent = " +
            createParent + "\n   replication = " + replication + "\n   blockSize = " + blockSize);

    if (!checkPathLength(src)) {
      throw new IOException(
              "create: Pathname too long.  Limit " + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    // I don't know what to use for this; the RPC server has a method for it, but I don't know if it applies to serverless case...
    String clientMachine = "";

    HdfsFileStatus stat = namesystem.startFile(
            src, new PermissionStatus(getRemoteUser().getShortUserName(), null, masked),
            clientName, clientMachine, flag, createParent, replication, blockSize, supportedVersions);

    // Currently, it is impossible to pass null for EncodingPolicy, but pretending it's possible for now...
    if (policy != null) {
      if (!namesystem.isErasureCodingEnabled()) {
        throw new IOException("Requesting encoding although erasure coding was disabled");
      }
      namesystem.addEncodingStatus(src, policy,
              EncodingStatus.Status.ENCODING_REQUESTED, false);
    }

    return stat;
  }

  private FsServerDefaults getServerDefaults(TaskArguments fsArgs) {
    EntityManager.toggleMetadataCacheReads(true);
    return this.namesystem.getServerDefaults();
  }

  private void renewLease(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String clientName = fsArgs.getString("clientName"); // fsArgs.getString("clientName"); // fsArgs.getAsJsonPrimitive("clientName").getAsString();
    this.namesystem.renewLease(clientName);
  }

  private long[] getStats(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(true);
    return this.namesystem.getStats();
  }

  private DatanodeInfo[] getDatanodeReport(TaskArguments fsArgs) throws AccessControlException {
    EntityManager.toggleMetadataCacheReads(true);
    int typeOrdinal = fsArgs.getInt("type"); // fsArgs.getAsJsonPrimitive("type").getAsInt();
    HdfsConstants.DatanodeReportType type = HdfsConstants.DatanodeReportType.values()[typeOrdinal];

    return namesystem.datanodeReport(type);
  }

  private boolean delete(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    //LOG.info("Unpacking arguments for the DELETE operation now...");

    //String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String src = fsArgs.getString("src");
    boolean recursive = fsArgs.getBoolean("recursive");
    //boolean recursive = fsArgs.getAsJsonPrimitive("recursive").getAsBoolean();

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
              "*DIR* Namenode.delete: src=" + src + ", recursive=" + recursive);
    }

    boolean ret;
    ret = namesystem.delete(src, recursive);

    if (ret) {
      metrics.incrDeleteFileOps();
    }
    return ret;
  }

  public boolean truncate(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String clientName = fsArgs.getString("clientName"); // fsArgs.getAsJsonPrimitive("clientName").getAsString();
    long newLength = fsArgs.getLong("newLength"); // fsArgs.getAsJsonPrimitive("newLength").getAsLong();

    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.truncate: " + src + " to " +
              newLength);
    }
    String clientMachine = NameNodeRpcServer.getClientMachine();
    try {
      return namesystem.truncate(
              src, newLength, clientName, clientMachine, now());
    } finally {
      metrics.incrFilesTruncated();
    }
  }

  private void rename(TaskArguments fsArgs) throws IOException {
    EntityManager.toggleMetadataCacheReads(false);
    //LOG.info("Unpacking arguments for the RENAME operation now...");

    String src = fsArgs.getString("src"); // fsArgs.getAsJsonPrimitive("src").getAsString();
    String dst = fsArgs.getString("dst"); // fsArgs.getAsJsonPrimitive("dst").getAsString();

    org.apache.hadoop.fs.Options.Rename[] options = fsArgs.getObjectArray("options");
//    JsonArray optionsArr = fsArgs.getAsJsonArray("options");
//
//    org.apache.hadoop.fs.Options.Rename[] options = new org.apache.hadoop.fs.Options.Rename[optionsArr.size()];
//
//    for (int i = 0; i < optionsArr.size(); i++) {
//      int renameOptionOrdinal = optionsArr.get(i).getAsInt();
//      options[i] = org.apache.hadoop.fs.Options.Rename.values()[renameOptionOrdinal];
//    }

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " + MAX_PATH_LENGTH +
                      " characters, " + MAX_PATH_DEPTH + " levels.");
    }

    RetryCacheEntry cacheEntry = LightWeightCacheDistributed.getTransactional();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }

    boolean success = false;
    try {
      namesystem.renameTo(src, dst, options);
      success = true;
    } finally {
      LightWeightCacheDistributed.putTransactional(success);
    }
    metrics.incrFilesRenamed();
  }

  private static CommandLine parseMainArguments(String[] args) {
    Options options = new Options();

    Option commandLineArgumentsOption = new Option("c", "command-line-args", true,
            "A String containing the command-line arguments for the NameNode.");
    commandLineArgumentsOption.setRequired(false);

    Option operationOption = new Option("o", "op", true,
            "The file system operation to perform.");
    operationOption.setRequired(true);

    Option fsArgsOption = new Option("f", "fsArgs", true,
            "JSON formatted as a String containing the arguments for the specified file system operation.");
    fsArgsOption.setRequired(false);

    CommandLineParser parser = new GnuParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
    }

    return cmd;
  }

  /**
   * httpServer
   */
  protected NameNodeHttpServer httpServer;
  private Thread emptier;
  /**
   * only used for testing purposes
   */
  protected boolean stopRequested = false;
  /**
   * Registration information of this name-node
   */
  protected NamenodeRegistration nodeRegistration;
  /**
   * Activated plug-ins.
   */
  private List<ServicePlugin> plugins;

  private NameNodeRpcServer rpcServer;

  private JvmPauseMonitor pauseMonitor;

  protected LeaderElection leaderElection;

  protected RevocationListFetcherService revocationListFetcherService;
  /**
   * for block report load balancing
   */
  private BRTrackingService brTrackingService;

  /**
   * Metadata cleaner service. Cleans stale metadata left my dead NNs
   */
  private MDCleaner mdCleaner;
  static long failedSTOCleanDelay = 0;
  long slowSTOCleanDelay = 0;

  private ObjectName nameNodeStatusBeanName;
  protected final Tracer tracer;
  protected final TracerConfigurationManager tracerConfigurationManager;

  /**
   * The service name of the delegation token issued by the namenode. It is
   * the name service id in HA mode, or the rpc address in non-HA mode.
   */
  private String tokenServiceName;

  /**
   * Format a new filesystem. Destroys any filesystem that may already exist
   * at this location.  *
   */
  public static void format(Configuration conf) throws IOException {
    formatHdfs(conf, false, true);
  }

  static NameNodeMetrics metrics;
  private static final StartupProgress startupProgress = new StartupProgress();

  /**
   * Return the {@link FSNamesystem} object.
   *
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  @VisibleForTesting
  public void setNamesystem(FSNamesystem fsNamesystem) {
    this.namesystem = fsNamesystem;
  }

  public NamenodeProtocols getRpcServer() {
    return rpcServer;
  }

  static void initMetrics(Configuration conf, NamenodeRole role) {
    metrics = NameNodeMetrics.create(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return metrics;
  }

  /**
   * Returns object used for reporting namenode startup progress.
   *
   * @return StartupProgress for reporting namenode startup progress
   */
  public static StartupProgress getStartupProgress() {
    return startupProgress;
  }

  /**
   * Return the service name of the issued delegation token.
   *
   * @return The name service id in HA-mode, or the rpc address in non-HA mode
   */
  public String getTokenServiceName() { return tokenServiceName; }

  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }

  /**
   * Set the configuration property for the service rpc address to address
   */
  public static void setServiceAddress(Configuration conf,
                                           String address) {
    LOG.info("Setting ADDRESS {}", address);
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
  }

  /**
   * Fetches the address for services to use when connecting to namenode based
   * on the value of fallback returns null if the special address is not
   * specified or returns the default namenode address to be used by both
   * clients and services. Services here are datanodes, backup node, any non
   * client connection
   */
  public static InetSocketAddress getServiceAddress(Configuration conf,
                                                        boolean fallback) {
    String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return fallback ? getAddress(conf) : null;
    }
    return getAddress(addr);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    return getAddress(filesystemURI);
  }

  /**
   * TODO:FEDERATION
   *
   * @param filesystemURI
   * @return address of file system
   */
  public static InetSocketAddress getAddress(URI filesystemURI) {
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!HdfsConstants.HDFS_URI_SCHEME
        .equalsIgnoreCase(filesystemURI.getScheme()) &&
        !HdfsConstants.ALTERNATIVE_HDFS_URI_SCHEME.equalsIgnoreCase(filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          HdfsConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":" + port);
    return URI.create(
        HdfsConstants.HDFS_URI_SCHEME + "://" + namenode.getHostName() +
            portString);
  }

  //
  // Common NameNode methods implementation for the active name-node role.
  //
  public NamenodeRole getRole() {
    if (leaderElection != null && leaderElection.isLeader()) {
      return NamenodeRole.LEADER_NAMENODE;
    }
    return NamenodeRole.NAMENODE;
  }

  boolean isRole(NamenodeRole that) {
    NamenodeRole currentRole = getRole();
    return currentRole.equals(that);
  }

  /**
   * Given a configuration get the address of the service rpc server If the
   * service rpc is not configured returns null
   */
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    return ServerlessNameNode.getServiceAddress(conf, false);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    return getAddress(conf);
  }

  /** Given a configuration get the bind host of the service rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getServiceRpcServerBindHost(Configuration conf) {
    String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }

  /** Given a configuration get the bind host of the client rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getRpcServerBindHost(Configuration conf) {
    String addr = conf.getTrimmed(DFS_NAMENODE_RPC_BIND_HOST_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }

  /**
   * Modifies the configuration passed to contain the service rpc address
   * setting
   */
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress serviceRPCAddress) {
    setServiceAddress(conf, NetUtils.getHostPortString(serviceRPCAddress));
  }

  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress rpcAddress) {
    FileSystem.setDefaultUri(conf, getUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return getHttpAddress(conf);
  }

  /**
   * HTTP server address for binding the endpoint. This method is
   * for use by the NameNode and its derivatives. It may return
   * a different address than the one that should be used by clients to
   * connect to the NameNode. See
   * {@link DFSConfigKeys#DFS_NAMENODE_HTTP_BIND_HOST_KEY}
   *
   * @param conf
   * @return
   */
  protected InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpServerAddress(conf);
    // If DFS_NAMENODE_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_NAMENODE_HTTP_ADDRESS_KEY.
    final String bindHost = conf.getTrimmed(DFS_NAMENODE_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }
    return bindAddress;
  }

  /**
   * @return the NameNode HTTP address
   */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_HTTP_ADDRESS_DEFAULT));
  }

  protected void loadNamesystem(Configuration conf, int deploymentNumber) throws IOException {
    this.namesystem = FSNamesystem.loadFromDisk(conf, this, deploymentNumber);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() throws IOException {
    nodeRegistration = new NamenodeRegistration(
        NetUtils.getHostPortString(rpcServer.getRpcAddress()),
        NetUtils.getHostPortString(getHttpAddress()),
        StorageInfo.getStorageInfoFromDB(),
        getRole());   //HOP change. previous code was getFSImage().getStorage()
    return nodeRegistration;
  }

  /* optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Login as the configured user for the NameNode.
   */
  void loginAsNameNodeUser(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    SecurityUtil
        .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
            socAddr.getHostName());
  }

  /**
   * Initialize name-node.
   *
   * @param conf
   *     the configuration
   */
  protected void initialize(Configuration conf) throws Exception {
    if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
      String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
        if (intervals != null)
          conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS, intervals);
      }

    LOG.debug("Initializing NameNode now...");
    Instant initStart = Instant.now();

    this.heartBeatInterval = conf.getLong(DFS_HEARTBEAT_INTERVAL_KEY,
            DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;

    UserGroupInformation.setConfiguration(conf);
    loginAsNameNodeUser(conf);

    Instant securityStartEnd = Instant.now();
    Duration securityDuration = Duration.between(initStart, securityStartEnd);
    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Finished security start-up in " + DurationFormatUtils.formatDurationHMS(securityDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - -");
    }

    HdfsStorageFactory.setConfiguration(conf);

    Instant storageFactorySetup = Instant.now();
    Duration storageFactoryDuration = Duration.between(securityStartEnd, storageFactorySetup);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Finished configuring HdfsStorageFactory in " +
              DurationFormatUtils.formatDurationHMS(storageFactoryDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - -");
    }

    Instant nameNodeInitStart = Instant.now();

    // The existing code uses longs for NameNode IDs, so I'm just using this to generate a random ID.
    // This should be sufficiently random for our purposes. I don't think we'll encounter collisions.
    // Note, Long.MAX_VALUE in binary is 0111111111111111111111111111111111111111111111111111111111111111
    // https://stackoverflow.com/questions/15184820/how-to-generate-unique-positive-long-using-uuid
    this.nameNodeID = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;

    if (LOG.isDebugEnabled()) LOG.debug("Assigned new NN instance ID " + nameNodeID);

    this.txAckTimeout =
            conf.getInt(SERVERLESS_TRANSACTION_ACK_TIMEOUT, SERVERLESS_TRANSACTION_ACK_TIMEOUT_DEFAULT);

    this.serverlessEndpointBase = conf.get(SERVERLESS_ENDPOINT, SERVERLESS_ENDPOINT_DEFAULT);
    this.serverlessInvoker = ServerlessInvokerFactory.getServerlessInvoker(
            conf.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT));
    this.serverlessInvoker.setConfiguration(conf, "NN" + deploymentNumber + "-" + getId(), this.serverlessEndpointBase);
    this.serverlessInvoker.setIsClientInvoker(false); // We are not a client.

    this.nameNodeTCPClient = new NameNodeTcpUdpClient(conf,this, nameNodeID, deploymentNumber, actionMemory);

    this.useNdbForConsistencyProtocol = conf.getBoolean(SERVERLESS_CONSISTENCY_USENDB, SERVERLESS_CONSISTENCY_USENDB_DEFAULT);

    this.spoofNdbOperations = conf.getBoolean(SERVERLESS_SPOOF_NDB, SERVERLESS_LOCAL_MODE_DEFAULT);

    if (spoofNdbOperations)
      LOG.warn("SPOOFING OF NDB OPERATIONS IS ENABLED.");
    else
      LOG.debug("Spoofing of NDB operations is DISABLED.");

    if (useNdbForConsistencyProtocol)
      LOG.debug("Using MySQL Cluster NDB for the consistency protocol.");
    else
      LOG.debug("Using ZooKeeper for the consistency protocol.");

    Instant serverlessInitDone = Instant.now();
    Duration serverlessInitDuration = Duration.between(nameNodeInitStart, serverlessInitDone);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Serverless-specific NN initialization completed in " +
              DurationFormatUtils.formatDurationHMS(serverlessInitDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    }

    if (localModeEnabled)
      numDeployments = 1;
    else
      numDeployments = conf.getInt(SERVERLESS_MAX_DEPLOYMENTS, SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

    if (LOG.isDebugEnabled()) LOG.debug("Number of unique serverless name nodes: " + numDeployments);

    int baseWaitTime = conf.getInt(DFSConfigKeys.DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_KEY,
            DFSConfigKeys.DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_DEFAULT);
    int retryCount = conf.getInt(DFSConfigKeys.DFS_NAMENODE_TX_RETRY_COUNT_KEY,
            DFSConfigKeys.DFS_NAMENODE_TX_RETRY_COUNT_DEFAULT);
    RequestHandler.setRetryBaseWaitTime(baseWaitTime);
    RequestHandler.setRetryCount(retryCount);

    final long updateThreshold = conf.getLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD_DEFAULT);
    final long  maxConcurrentBRs = conf.getLong( DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN,
            DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN_DEFAULT);
    final long brMaxProcessingTime = conf.getLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME,
            DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME_DEFAULT);
     this.brTrackingService = new BRTrackingService(updateThreshold, maxConcurrentBRs,
             brMaxProcessingTime);
    this.mdCleaner = MDCleaner.getInstance();
    failedSTOCleanDelay = conf.getLong(
            DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY,
            DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_DEFAULT);
    this.slowSTOCleanDelay = conf.getLong(
            DFSConfigKeys.DFS_SUBTREE_CLEAN_SLOW_OPS_LOCKS_DELAY_KEY,
            DFSConfigKeys.DFS_SUBTREE_CLEAN_SLOW_OPS_LOCKS_DELAY_DEFAULT);

    String fsOwnerShortUserName = UserGroupInformation.getCurrentUser()
        .getShortUserName();
    String superGroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);

    try {
      UsersGroups.addUser(fsOwnerShortUserName);
      UsersGroups.addGroup(superGroup);
      UsersGroups.addUserToGroup(fsOwnerShortUserName, superGroup);
    } catch (HopsUGException e){ }

    try {
      createAndStartCRLFetcherService(conf);
    } catch (Exception ex) {
      LOG.error("Error starting CRL fetcher service", ex);
      throw new IOException(ex);
    }

    try {
      ServerlessNameNode.initMetrics(conf, this.getRole());
    } catch (org.apache.hadoop.metrics2.MetricsException e) {
      LOG.warn("Encountered exception during initialization of NameNode metrics system.");
      e.printStackTrace();
    }
    StartupProgressMetrics.register(startupProgress);

    Instant intermediateInitDone = Instant.now();
    Duration intermediateInitDuration = Duration.between(serverlessInitDone, intermediateInitDone);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Intermediate NameNode initialization completed in " +
              DurationFormatUtils.formatDurationHMS(intermediateInitDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    }

    loadNamesystem(conf, deploymentNumber);

    Instant loadNamesystemDone = Instant.now();
    Duration loadNamesystemDuration = Duration.between(intermediateInitDone, loadNamesystemDone);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - -");
      LOG.debug("Loaded namesystem in " + DurationFormatUtils.formatDurationHMS(loadNamesystemDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - -");
    }

    // We need to do this AFTER the above call to `HdfsStorageFactory.setConfiguration(conf)`, as the ClusterJ/NDB
    // library is loaded during that call. If we try to create the event manager before that, we will get class
    // not found errors.
    ndbEventManager = DalDriver.loadEventManager(conf.get(DFS_EVENT_MANAGER_CLASS, DFS_EVENT_MANAGER_CLASS_DEFAULT));
    ndbEventManager.setConfigurationParameters(deploymentNumber, null, false, namesystem);

    // Note that we need to register the namesystem as an event listener with the event manager,
    // but the name system doesn't get loaded until a little later.
    eventManagerThread = new Thread(ndbEventManager);

    if (LOG.isDebugEnabled())
      LOG.debug("Started the NDB EventManager thread.");

    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(conf);
    pauseMonitor.start();

    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

    Instant metadataInitStart = Instant.now();

    Duration pauseMonitorAndEventManagerDuration = Duration.between(loadNamesystemDone, metadataInitStart);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Started the Event Manager and Pause Monitor in " +
              DurationFormatUtils.formatDurationHMS(pauseMonitorAndEventManagerDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    }

    this.zooKeeperClient = new SyncZKClient(conf);

    // Do NOT join the ZK ensemble until we are fully started up.
    // If we join the ZK ensemble and then encounter a critical error, other NNs will think there
    // exists some NN that is alive when in fact no such NN exists because we crashed later on in
    // our start-up process. We'll enter the ZK ensemble when we enter the active state at the very
    // end of the initialization process. (If we encounter an error that causes us to crash but not terminate,
    // our ephemeral node will not be deleted...)
    this.zooKeeperClient.connect();
    // Note that, since we haven't joined a group yet, we won't be considered active. So, we won't
    // actually be included in the initialization of the active NN list. We'll be added later after
    // we join our deployment's ZooKeeper group.
    refreshActiveNameNodesList();

    assert(this.activeNameNodes != null);
    assert(this.activeNameNodes.getActiveNodes() != null);

    Instant metadataInitEnd = Instant.now();
    Duration metadataInitDuration = Duration.between(metadataInitStart, metadataInitEnd);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Connected to ZooKeeper in " +
              DurationFormatUtils.formatDurationHMS(metadataInitDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    }

    startCommonServices(conf, this.activeNameNodes);
    Instant commonServiceEnd = Instant.now();
    Duration startCommonServiceDuration = Duration.between(metadataInitEnd, commonServiceEnd);

    if (LOG.isDebugEnabled()) {
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - -");
      LOG.debug("Started common NameNode services in " +
              DurationFormatUtils.formatDurationHMS(startCommonServiceDuration.toMillis()));
      LOG.debug("- - - - - - - - - - - - - - - - - - - - - - -");
    }

    if(isLeader()) { //if the newly started namenode is the leader then it means
      //that is cluster was restarted and we can reset the number of default
      // concurrent block reports
      HdfsVariables.setMaxConcurrentBrs(maxConcurrentBRs, null);
      createLeaseLocks(conf);

      Instant createLeaseLocksEnd = Instant.now();
      Duration createLeaseLocksDuration = Duration.between(commonServiceEnd, createLeaseLocksEnd);

      if (LOG.isDebugEnabled()) {
        LOG.debug("- - - - - - - - - - - - - - - - - - - - - - -");
        LOG.debug("Created lease locks in " +
                DurationFormatUtils.formatDurationHMS(createLeaseLocksDuration.toMillis()));
        LOG.debug("- - - - - - - - - - - - - - - - - - - - - - -");
      }
    }

    // in case of cluster upgrade the retry cache epoch is set to 0
    // update the epoch to correct value
    if (HdfsVariables.getRetryCacheCleanerEpoch() == 0){
      // -1 to ensure the entries in the current epoch are delete by the cleaner
      HdfsVariables.setRetryCacheCleanerEpoch(System.currentTimeMillis()/1000 - 1);
    }
  }

  public long getHeartBeatInterval() {
    return heartBeatInterval;
  }

  /**
   * Create the RPC server implementation. Used as an extension point for the
   * BackupNode.
   */
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new NameNodeRpcServer(conf, this);
  }

  /**
   * Start the services common to active and standby states
   *
   * @param activeNodeList The active nodes computed right when ZooKeeper first connects to the ensemble
   *                       as the Serverless NameNode is starting up.
   */
  private void startCommonServices(Configuration conf, SortedActiveNodeList activeNodeList) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
      LOG.debug("Starting common NameNode services now...");
      LOG.debug("=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
      LOG.debug("NOT starting the Leader Election Service.");
    }
    // startLeaderElectionService();

    startMDCleanerService();

    namesystem.startCommonServices(conf, activeNodeList);
    registerNNSMXBean();

    plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY, ServicePlugin.class);
    for (ServicePlugin p : plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
    /*LOG.info(getRole() + " RPC up at: " + rpcServer.getRpcAddress());
    if (rpcServer.getServiceRpcAddress() != null) {
      LOG.info(getRole() + " service RPC up at: " +
          rpcServer.getServiceRpcAddress());
    }*/
  }

  /**
   * Register NameNodeStatusMXBean
   */
  private void registerNNSMXBean() {
    nameNodeStatusBeanName = MBeans.register("NameNode", "NameNodeStatus", this);
  }

  @Override // NameNodeStatusMXBean
  public String getNNRole() {
    String roleStr = "";
    NamenodeRole role = getRole();
    if (null != role) {
      roleStr = role.toString();
    }
    return roleStr;
  }

  @Override // NameNodeStatusMXBean
  public String getHostAndPort() {
    return getNameNodeAddressHostPortString();
  }

  @Override // NameNodeStatusMXBean
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  /**
   * Return the Serverless Invoker object used by this NameNode.
   */
  public ServerlessInvokerBase getServerlessInvoker() { return serverlessInvoker; }

  public String getServerlessEndpointBase() { return serverlessEndpointBase; }

  /**
   * Return the EventManager instance.
   */
  public EventManager getNdbEventManager() {
    return ndbEventManager;
  }

  private void stopCommonServices() {
    if (leaderElection != null && leaderElection.isRunning()) {
      try {
        leaderElection.stopElectionThread();
      } catch (InterruptedException e) {
        LOG.warn("LeaderElection thread stopped",e);
      }
    }

    if (rpcServer != null) {
      rpcServer.stop();
    }
    if (namesystem != null) {
      namesystem.close();
    }
    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }
    if(mdCleaner != null){
      mdCleaner.stopMDCleanerMonitor();
    }

    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }

    if (revocationListFetcherService != null) {
      try {
        revocationListFetcherService.serviceStop();
      } catch (Exception ex) {
        LOG.warn("Exception while stopping CRL fetcher service, but we are shutting down anyway");
      }
    }

    stopHttpServer();
  }

  private void startTrashEmptier(final Configuration conf) throws IOException {
    long trashInterval =
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    if (trashInterval == 0) {
      return;
    } else if (trashInterval < 0) {
      throw new IOException(
          "Cannot start trash emptier with negative interval." + " Set " +
              FS_TRASH_INTERVAL_KEY + " to a positive value.");
    }

    // This may be called from the transitionToActive code path, in which
    // case the current user is the administrator, not the NN. The trash
    // emptier needs to run as the NN. See HDFS-3972.
    FileSystem fs =
        SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<FileSystem>() {
              @Override
              public FileSystem run() throws IOException {
                return FileSystem.get(conf);
              }
            });
    this.emptier =
        new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void stopTrashEmptier() {
    if (this.emptier != null) {
      emptier.interrupt();
      emptier = null;
    }
  }

  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new NameNodeHttpServer(conf, this, getHttpServerBindAddress(conf));
    httpServer.start();
    httpServer.setStartupProgress(startupProgress);
  }

  private void stopHttpServer() {
    try {
      if (httpServer != null) {
        httpServer.stop();
      }
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
    }
  }

  public boolean useNdbForConsistencyProtocol() {
    return useNdbForConsistencyProtocol;
  }

  /**
   * Start NameNode.
   * <p/>
   * The name-node can be started with one of the following startup options:
   * <ul>
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * @param conf
   *     confirguration
   * @throws IOException
   */
  public ServerlessNameNode(Configuration conf, String functionName, int actionMemory)
          throws Exception {
    this(conf, NamenodeRole.NAMENODE, functionName, actionMemory);
  }

  protected ServerlessNameNode(Configuration conf, NamenodeRole role, String functionName,
                               int actionMemory) throws Exception {
    this.functionName = functionName;

    if (LOG.isDebugEnabled())LOG.debug("Local mode: " + (localModeEnabled ? "ENABLED" : "DISABLED"));

    if (localModeEnabled)
      this.deploymentNumber = 0;
    else
      this.deploymentNumber = getFunctionNumberFromFunctionName(functionName);

    this.actionMemory = actionMemory;

    if (this.deploymentNumber < 0)
      throw new IOException("Failed to extract valid deployment number from function name '" +
              functionName + "'");

    if (LOG.isDebugEnabled()) LOG.debug("We are function '" + this.functionName + "' from deployment #" + this.deploymentNumber + ".");
    // Subtract six seconds (i.e., 6000 milliseconds) to account for invocation overheads and other start-up times.
    // The default DN heartbeat interval (and therefore, StorageReport interval) is three seconds, so this should
    // ensure that the NN finds at least 1-2 storage reports, which can be used to bootstrap the DN storages.
    this.creationTime = System.currentTimeMillis() - 6000;
    this.tracer = new Tracer.Builder("NameNode").
      conf(TraceUtils.wrapHadoopConf(NAMENODE_HTRACE_PREFIX, conf)).
      build();
    this.tracerConfigurationManager =
      new TracerConfigurationManager(NAMENODE_HTRACE_PREFIX, conf);
    this.conf = conf;
    try {
      initializeGenericKeys(conf);
      Instant initStart = Instant.now();
      initialize(conf);
      Instant initEnd = Instant.now();
      Duration initDuration = Duration.between(initStart, initEnd);
      if (LOG.isDebugEnabled()) LOG.debug("NameNode initialization completed. Time elapsed: " +
              DurationFormatUtils.formatDurationHMS(initDuration.toMillis()));
      this.started.set(true);
      Instant activeStateStart = Instant.now();
      enterActiveState();
      Instant activeStateEnd = Instant.now();
      Duration enterActiveStateDuration = Duration.between(activeStateStart, activeStateEnd);
      if (LOG.isDebugEnabled()) LOG.debug("NameNode entered active state. Time elapsed: " +
              DurationFormatUtils.formatDurationHMS(enterActiveStateDuration.toMillis()));
    } catch (Exception e) {
      this.stop();
      throw e;
    }
  }


  /**
   * Wait for service to finish. (Normally, it runs forever.)
   */
  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception ", ie);
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    synchronized (this) {
      if (stopRequested) {
        return;
      }
      stopRequested = true;
    }
    try {
      exitActiveServices();
    } catch (ServiceFailedException e) {
      LOG.warn("Encountered exception while exiting state ", e);
    } finally {
      stopCommonServices();
      if (metrics != null) {
        metrics.shutdown();
      }
      if (namesystem != null) {
        namesystem.shutdown();
      }
      if (nameNodeStatusBeanName != null) {
        MBeans.unregister(nameNodeStatusBeanName);
        nameNodeStatusBeanName = null;
      }
    }
    tracer.close();
  }


  synchronized boolean isStopRequested() {
    return stopRequested;
  }

  /**
   * Returns the number of this serverless function, given the name.
   *
   * If this returns -1, then that means it could not extract the number from the name.
   * A "correct" number will always be >= 0.
   * @param functionName The name of this serverless function.
   * @return The number, which will be >= 0 if valid, otherwise < 0.
   */
  public static int getFunctionNumberFromFunctionName(String functionName) {
    Pattern lastIntPattern = Pattern.compile("[^0-9]+([0-9]+)$");
    Matcher matcher = lastIntPattern.matcher(functionName);
    if (matcher.find()) {
      String someNumberStr = matcher.group(1);
      return Integer.parseInt(someNumberStr);
    }

    return -1;
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() throws IOException {
    return namesystem.isInSafeMode();
  }

  /**
   * @return NameNode RPC address
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcServer.getRpcAddress();
  }

  /**
   * @return NameNode RPC address in "host:port" string form
   */
  public String getNameNodeAddressHostPortString() {
    return NetUtils.getHostPortString(rpcServer.getRpcAddress());
  }

  /**
   * @return NameNode service RPC address if configured, the NameNode RPC
   * address otherwise
   */
  public InetSocketAddress getServiceRpcAddress() {
    final InetSocketAddress serviceAddr = rpcServer.getServiceRpcAddress();
    return serviceAddr == null ? rpcServer.getRpcAddress() : serviceAddr;
  }

  /**
   * @return NameNode HTTP address, used by the Web UI, image transfer,
   *    and HTTP-based file system clients like WebHDFS
   */
  public InetSocketAddress getHttpAddress() {
    return httpServer.getHttpAddress();
  }

  /**
   * @return NameNode HTTPS address, used by the Web UI, image transfer,
   *    and HTTP-based file system clients like WebHDFS
   */
  public InetSocketAddress getHttpsAddress() {
    return httpServer.getHttpsAddress();
  }

  /**
   * Verify that configured directories exist, then Interactively confirm that
   * formatting is desired for each existing directory and format them.
   *
   * @param conf
   * @param force
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean formatHdfs(Configuration conf, boolean force,
      boolean isInteractive) throws IOException {
    LOG.debug("Formatting HDFS now...");

    initializeGenericKeys(conf);
    checkAllowFormat(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil
          .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
              socAddr.getHostName());
    }

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if (clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = StorageInfo.newClusterID();
    }

    try {
      HdfsStorageFactory.setConfiguration(conf);
      if (force) {
        HdfsStorageFactory.formatHdfsStorageNonTransactional();
      } else {
        HdfsStorageFactory.formatHdfsStorage();
      }
      StorageInfo.storeStorageInfoToDB(clusterId, Time.now());  //this adds new row to the db
      UsersGroups.createSyncRow();
      createLeaseLocks(conf);
    } catch (StorageException e) {
      throw new RuntimeException(e.getMessage());
    }

    return false;
  }

  /**
   * Schedule all of the {@link WriteAcknowledgement} instances in {@code acksToDelete} to be removed from
   * intermediate storage (e.g., NDB).
   */
  public void enqueueAcksForDeletion(Collection<WriteAcknowledgement> acksToDelete) {
    this.executionManager.enqueueAcksForDeletion(acksToDelete);
  }

  @VisibleForTesting
  public static boolean formatAll(Configuration conf) throws IOException {
    LOG.warn("Formatting HopsFS and HopsYarn");
    initializeGenericKeys(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil
              .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
                      socAddr.getHostName());
    }

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if (clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = StorageInfo.newClusterID();
    }

    try {
      HdfsStorageFactory.setConfiguration(conf);
//      HdfsStorageFactory.formatAllStorageNonTransactional();
      HdfsStorageFactory.formatStorage();
      StorageInfo.storeStorageInfoToDB(clusterId, Time.now());  //this adds new row to the db
    } catch (StorageException e) {
      throw new RuntimeException(e.getMessage());
    }

    return false;
  }

  public static void checkAllowFormat(Configuration conf) throws IOException {
    if (!conf.getBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY,
        DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT)) {
      throw new IOException(
          "The option " + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY +
              " is set to false for this filesystem, so it " +
              "cannot be formatted. You will need to set " +
              DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY + " parameter " +
              "to true in order to format this filesystem");
    }
  }

  /**
   * Write the metadata for this Serverless NameNode instance to NDB.
   *
   * This function will delete existing metadata associated with whatever serverless function we're running on, if
   * any exists. Then it will add the new metadata. If there is no existing metadata associated with whatever
   * serverless function we're running on, then the new metadata is simply added.
   */
  private void writeMetadataToIntermediateStorage() throws StorageException {
    LOG.debug("Writing Serverless NameNode metadata to NDB.");

    ServerlessNameNodeDataAccess<ServerlessNameNodeMeta> dataAccess =
            (ServerlessNameNodeDataAccess)HdfsStorageFactory.getDataAccess(ServerlessNameNodeDataAccess.class);

    // Hard-coding the replica ID for now as it is essentially a place-holder.
    ServerlessNameNodeMeta serverlessNameNodeMeta =
            new ServerlessNameNodeMeta(getId(), functionName, "Replica1", System.currentTimeMillis());

    dataAccess.replaceServerlessNameNode(serverlessNameNodeMeta);
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  @VisibleForTesting
  public static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.NO_OF_CONCURRENT_BLOCK_REPORTS.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.NO_OF_CONCURRENT_BLOCK_REPORTS;
        String msg = "Specify a maximum number of concurrent blocks that the NameNodes can process.";
            if ((i + 1) >= argsLen) {
              // if no of blks not specified then return null
              LOG.error(msg);
              return null;
            }
            // Make sure an id is specified and not another flag
            long maxBRs = 0;
            try{
              maxBRs = Long.parseLong(args[i+1]);
              if(maxBRs < 1){
                LOG.error("The number should be >= 1.");
              return null;
              }
            }catch(NumberFormatException e){
              return null;
            }
            startOpt.setMaxConcurrentBlkReports(maxBRs);
            return startOpt;
      }

      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i >= argsLen) {
              // if no cluster id specified, return null
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            String clusterId = args[i];
            // Make sure an id is specified and not another flag
            if (clusterId.isEmpty() ||
                clusterId.equalsIgnoreCase(StartupOption.FORCE.getName()) ||
                clusterId
                    .equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
              LOG.error("Must specify a valid cluster ID after the " +
                  StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            startOpt.setClusterId(clusterId);
          }

          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForceFormat(true);
          }

          if (args[i]
              .equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
            startOpt.setInteractiveFormat(false);
          }
        }
      } else if (StartupOption.FORMAT_ALL.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT_ALL;
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.GENCLUSTERID;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
        // might be followed by two args
        if (i + 2 < argsLen &&
            args[i + 1].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
          i += 2;
          startOpt.setClusterId(args[i]);
        }
      } else if (StartupOption.ROLLINGUPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLINGUPGRADE;
        ++i;
        if (i >= argsLen) {
          LOG.error("Must specify a rolling upgrade startup option "
              + RollingUpgradeStartupOption.getAllOptionString());
          return null;
        }
        startOpt.setRollingUpgradeStartupOption(args[i]);
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else if (StartupOption.BOOTSTRAPSTANDBY.getName()
          .equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BOOTSTRAPSTANDBY;
        return startOpt;
      } else if (StartupOption.INITIALIZESHAREDEDITS.getName()
          .equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INITIALIZESHAREDEDITS;
        for (i = i + 1; i < argsLen; i++) {
          if (StartupOption.NONINTERACTIVE.getName().equals(args[i])) {
            startOpt.setInteractiveFormat(false);
          } else if (StartupOption.FORCE.getName().equals(args[i])) {
            startOpt.setForceFormat(true);
          } else {
            LOG.error("Invalid argument: " + args[i]);
            return null;
          }
        }
        return startOpt;
      } else if (StartupOption.RECOVER.getName().equalsIgnoreCase(cmd)) {
        if (startOpt != StartupOption.REGULAR) {
          throw new RuntimeException(
              "Can't combine -recover with " + "other startup options.");
        }
        startOpt = StartupOption.RECOVER;
        while (++i < argsLen) {
          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForce(MetaRecoveryContext.FORCE_FIRST_CHOICE);
          } else {
            throw new RuntimeException("Error parsing recovery options: " +
                "can't understand option \"" + args[i] + "\"");
          }
        }
      } else {
        return null;
      }
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_NAMENODE_STARTUP_KEY, opt.name());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(
        conf.get(DFS_NAMENODE_STARTUP_KEY, StartupOption.REGULAR.toString()));
  }

  /**
   * Create the ServerlessNameNode instance.
   * @param actionMemory The amount of RAM (in megabytes) that this function has been allocated. Used when
   *                     determining the number of active TCP connections that this NameNode can have at once, as
   *                     each TCP connection has two relatively-large buffers. If the NN creates too many TCP
   *                     connections at once, then it might crash due to OOM errors.
   * @param asServerless Only true if we're running in a serverless function (or in localMode). If we're being
   *                     called from the proper main method of this class, then the 'asServerless' field is false.
   *                     This prevents us from using the hard-coded config file paths, as they won't work unless
   *                     we're in the Docker image used by serverless functions.
   */
  public static ServerlessNameNode createNameNode(String[] argv, Configuration conf, String functionName,
                                                  int actionMemory, boolean asServerless)
          throws Exception {
    LOG.info("createNameNode " + Arrays.asList(argv));
    if (conf == null) {
      conf = new HdfsConfiguration();
    }

    if (asServerless) {
      // TODO: Make this not hard-coded. It doesn't have to be hard-coded for OpenWhisk, but Nuclio doesn't
      //       seem to be finding the configuration files...?
      conf.addResource(new File("/conf/hdfs-site.xml").toURI().toURL());
      conf.addResource(new File("/conf/core-site.xml").toURI().toURL());
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      return null;
    }
    setStartupOption(conf, startOpt);

    if (LOG.isDebugEnabled()) LOG.debug("Start Option given as: " + startOpt.getName() + ", " + startOpt);

    // TODO: We could eventually have some of these NOT terminate and instead return a message that could
    //       then be passed all the way back to the user/client who invoked the serverless NN initially.
    switch (startOpt) {
      //HOP
      case NO_OF_CONCURRENT_BLOCK_REPORTS:
        HdfsVariables.setMaxConcurrentBrs(startOpt.getMaxConcurrentBlkReports(), conf);
        LOG.info("Setting concurrent block reports processing to "+startOpt
                .getMaxConcurrentBlkReports());
        return null;
      case FORMAT: {
        boolean aborted = formatHdfs(conf, startOpt.getForceFormat(),
            startOpt.getInteractiveFormat());
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case FORMAT_ALL: {
        boolean aborted = formatAll(conf);
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case GENCLUSTERID: {
        System.err.println("Generating new cluster id:");
        LOG.info(StorageInfo.newClusterID());
        terminate(0);
        return null;
      }
      case FINALIZE: {
        throw new UnsupportedOperationException(
            "HOP: FINALIZE is not supported anymore");
      }
      case BOOTSTRAPSTANDBY: {
        throw new UnsupportedOperationException(
            "HOP: BOOTSTRAPSTANDBY is not supported anymore");
      }
      case INITIALIZESHAREDEDITS: {
        throw new UnsupportedOperationException(
            "HOP: INITIALIZESHAREDEDITS is not supported anymore");
      }
      case BACKUP:
      case CHECKPOINT: {
        throw new UnsupportedOperationException(
            "HOP: BACKUP/CHECKPOINT is not supported anymore");
      }
      case RECOVER: {
        new UnsupportedOperationException(
            "Hops. Metadata recovery is not supported");
        return null;
      }
      default: {

//        String additionalLibsFolder = conf.get(SERVERLESS_ADDITIONAL_LIBS_PATH, SERVERLESS_ADDITIONAL_LIBS_PATH_DEFAULT);
//        if (additionalLibsFolder != null) {
//          LOG.debug("Adding additional libraries folder to classpath. Folder path: '" + additionalLibsFolder + "'");
//          URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
//          Class<URLClassLoader> urlClass = URLClassLoader.class;
//          File f = new File(additionalLibsFolder);
//          URL u = f.toURI().toURL();
//          Method method = urlClass.getDeclaredMethod("addURL", URL.class);
//          method.setAccessible(true);
//          method.invoke(urlClassLoader, u);
//        }

        // DefaultMetricsSystem.initialize("NameNode");
        // Make sure the NameNode does not already exist.
        assert(ServerlessNameNode.tryGetNameNodeInstance(false) == null);
        return new ServerlessNameNode(conf, functionName, actionMemory);
      }
    }
  }

  public static void initializeGenericKeys(Configuration conf) {
    // If the RPC address is set use it to (re-)configure the default FS
    if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" +
          conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      if (LOG.isDebugEnabled()) LOG.debug("Setting " + FS_DEFAULT_NAME_KEY + " to " + defaultUri.toString());
    }
  }

  /**
   * Return True if we should cache this INode locally, otherwise return False.
   * @param inode The INode in question.
   * @return True if we should cache this INode locally, otherwise returns False.
   */
  public boolean shouldCacheLocally(INode inode) {
    return getMappedDeploymentNumber(inode) == deploymentNumber;
  }

  /**
   * Return True if we should cache this INode locally, otherwise return False.
   * @param parentINodeId The parent INode ID of the node we're inquiring about.
   * @return True if we should cache this INode locally, otherwise returns False.
   */
  public boolean shouldCacheLocally(long parentINodeId) {
    return getMappedDeploymentNumber(parentINodeId) == deploymentNumber;
  }

  /**
   * Get the serverless function number of the NameNode that should cache this INode.
   * @param inode The INode in question.
   * @return The number of the serverless function responsible for caching this INode.
   */
  public int getMappedDeploymentNumber(INode inode) {
    return consistentHash(inode.getParentId(), numDeployments);
    //return consistentHash(inode.getFullPathName().hashCode(), numUniqueServerlessNameNodes);
  }

  /**
   * Get the serverless function number of the NameNode that should cache this INode.
   * @param parentINodeId The parent INode ID of the node we're inquiring about.
   * @return The number of the serverless function responsible for caching this INode.
   */
  public int getMappedDeploymentNumber(long parentINodeId) {
    return consistentHash(parentINodeId, numDeployments);
  }

  /**
   * Get the serverless function number of the NameNode that should cache this file/directory.
   * @param path Fully-qualified path to the target file/directory.
   * @return The number of the serverless function responsible for caching this file/directory.
   */
  public int getMappedDeploymentNumber(String path) throws IOException {
    INode inode = getINodeForCache(path);

    if (inode == null) {
      LOG.warn("INode for path '" + path + "' is null. If we're not creating a directory right now, then that's a problem.");

      // If we're creating a directory, then there's not much we can do here. So, we'll just create the directory
      // ourselves. Since we're creating an entirely new INode, there's not going to be any cache consistency issues.
      return deploymentNumber;
    }

    return consistentHash(inode.getParentId(), numDeployments);
    //return consistentHash(path.hashCode(), numUniqueServerlessNameNodes);
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, ServerlessNameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      // StringUtils.startupShutdownMessage(ServerlessNameNode.class, argv, LOG);
      ServerlessNameNode namenode = createNameNode(argv, null, "LocalVMNameNode0",
              (int) Runtime.getRuntime().maxMemory(), false);
      if (namenode != null) {
        namenode.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start namenode.", e);
      terminate(1, e);
    }
  }

  private void enterActiveState() throws ServiceFailedException {
    try {
      startActiveServicesInternal();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start active services", e);
    }
  }

  private void startActiveServicesInternal() throws IOException {
    try {
      this.zooKeeperClient.createAndJoinGroup(this.functionName, String.valueOf(this.nameNodeID), namesystem);
      namesystem.startActiveServices();
      startTrashEmptier(conf);
      // eventManagerThread.start();

      // Create the thread and tell it to run!
      executionManager = new ExecutionManager(conf, this);
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }

  private void exitActiveServices() throws ServiceFailedException {
    try {
      stopActiveServicesInternal();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop active services", e);
    }
  }

  private void stopActiveServicesInternal() throws IOException {
    try {
      if (namesystem != null) {
        namesystem.stopActiveServices();
      }
      stopTrashEmptier();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }


  /**
   * Shutdown the NN immediately in an ungraceful way. Used when it would be
   * unsafe for the NN to continue operating, e.g. during a failed HA state
   * transition.
   *
   * @param t
   *     exception which warrants the shutdown. Printed to the NN log
   *     before exit.
   * @throws ExitException
   *     thrown only for testing.
   */
  protected synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring NN shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.error(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }

    if (this.zooKeeperClient != null) {
      try {
        if (LOG.isDebugEnabled()) LOG.debug("Attempting to disconnect from ZooKeeper ensemble before ungraceful termination...");
        this.zooKeeperClient.leaveGroup("namenode" + deploymentNumber, Long.toString(getId()), true);
      } catch (Exception e) {
        LOG.error("Exception encountered while trying to disconnect from ZooKeeper:", e);
      }
    }

    terminate(1, t);
  }

  /**
   * Returns the id of this namenode
   *
   * @throws IllegalStateException if the ID is requested prior to being set.
   */
  public long getId() throws IllegalStateException {
    if (this.nameNodeID == -1)
      throw new IllegalStateException("NameNode ID requested before the ID has been set.");

    return this.nameNodeID;
  }

  /**
   * Return the name of this Serverless Name Node.
   */
  public String getFunctionName() {
    return this.functionName;
  }

  /**
   * Return the deployment number of the serverless function in which we're running.
   */
  public int getDeploymentNumber() {
    return this.deploymentNumber;
  }

  /**
   * Return the {@link LeaderElection} object.
   *
   * @return {@link LeaderElection} object.
   */
  public LeaderElection getLeaderElectionInstance() {
    return leaderElection;
  }

  // TODO: Figure out what to do about leader NN semantics for serverless.
  // TODO: One possible solution is to have a table in NDB where the leader writes that it is leader.
  //       Other NNs check if it is still alive, and if not, then they act as leader by replacing the value.
  //       This could be done atomically using a transaction.
  public boolean isLeader() {
    if (activeNameNodes != null) {
      if (activeNameNodes.getActiveNodes().size() <= 1)
        return true;
      return false;
    }

    return true;
  }

  public ActiveNode getNextNamenodeToSendBlockReport(final long noOfBlks, DatanodeID nodeID) throws IOException {
    if (leaderElection.isLeader()) {
      DatanodeDescriptor node = namesystem.getBlockManager().getDatanodeManager().getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        throw new IOException(
            "ProcessReport from dead or unregistered node: " + nodeID+ ". "
                    + (node != null ? ("The node is alive : " + node.isAlive) : "The node is null "));
      }
      if (LOG.isDebugEnabled()) LOG.debug("NN Id: " + leaderElection.getCurrentId() + ") Received request to assign" +
              " block report work ("+ noOfBlks + " blks) ");
      ActiveNode an = brTrackingService.assignWork(getActiveNameNodes(),
              nodeID.getXferAddr(), noOfBlks);
      return an;
    } else {
      String msg = "NN Id: " + leaderElection.getCurrentId() + ") Received request to assign" +
              " work (" + noOfBlks + " blks). Returning null as I am not the leader NN";
      if (LOG.isDebugEnabled()) LOG.debug(msg);
      throw new BRLoadBalancingNonLeaderException(msg);
    }
  }

  /**
   * IMPORTANT: We no longer use the activeNameNodes parameter!
   *
   * We retrieve the global/singleton NameNode instance from the OpenWhiskHandler. We then retrieve
   * the list of active nodes directly from there, as they could have changed at any point. Thus, we
   * are using the most up-to-date information we have available.
   *
   * We first check if the specified NN is contained within our local list of active NameNodes.
   * If it is not contained within that list, then we check the other deployments.
   *
   * @param namenodeId The ID of the NameNode whose existence we're interested in.
   *
   * @return True if the NameNode is alive in any deployment.
   */
  public static boolean isNameNodeAlive(long namenodeId, Collection<ActiveNode> activeNamenodes) {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);

    if (LOG.isDebugEnabled()) LOG.debug("Checking if NameNode " + namenodeId + " is alive...");

    Collection<ActiveNode> activeNodes;

    // The instance can be null when we're first starting up.
    if (instance != null) {
      SortedActiveNodeList activeNodeList = instance.getActiveNameNodes();
      if (activeNodeList == null)
        activeNodes = activeNamenodes;
      else {
        activeNodes = activeNodeList.getActiveNodes();

        if (activeNodes == null)
          activeNodes = activeNamenodes;
      }
    } else {
      activeNodes = activeNamenodes;
    }

    // First check local cache. This contains the NNs currently alive within our deployment.
    for (ActiveNode nameNode : activeNodes) {
      if (nameNode.getId() == namenodeId) {
        if (LOG.isDebugEnabled()) LOG.debug("NameNode " + namenodeId + " IS alive, according to our local records.");
        return true;
      }
    }

    // During start-up, the instance will be null. This happens if the NN starts up, checks ZK, and finds that
    // no other NameNodes are alive. In this case, it considers itself to be the leader and does some book-keeping.
    if (instance == null) {
      LOG.warn("The NameNode instance is null, so we cannot query ZooKeeper for all active NNs right now.");
      LOG.warn("In theory, we passed in state directly from ZooKeeper " +
              "(that we grabbed during initialization), so this should be okay...");
      return false;
    }

    // If the ID is our local ID, then return true, since we're clearly alive.
    if (namenodeId == instance.getId())
      return true;

    // If not in cache, then check ZooKeeper. We'll check for the existence of a persistent ZNode
    // in the permanent sub-group of each deployment. If one does not exist, then the NN is dead.
    ZKClient zkClient = instance.getZooKeeperClient();
    int numDeployments = instance.getNumDeployments();

    // If we encounter an exception, then we'll just be conservative and assume the NameNode is alive.
    boolean exceptionEncountered = false;

    for (int i = 0; i < numDeployments; i++) {
      try {
        boolean alive = zkClient.checkIfNameNodeIsAlive(i, String.valueOf(namenodeId));

        if (alive) {
          if (LOG.isDebugEnabled()) LOG.debug("NameNode " + namenodeId + " IS alive in deployment #" + i + " according to ZooKeeper.");
          return true;
        }
      } catch (Exception ex) {
        LOG.error("Exception encountered while checking deployment #" + i + " for existence of NN " +
                namenodeId + ": ", ex);
        exceptionEncountered = true;
        break;
      }
    }

    if (exceptionEncountered) {
      LOG.warn("Since exception was encountered while checking liveness of NN " + namenodeId +
              ", we'll assume it is alive for now...");
      return true;
    }

    if (LOG.isDebugEnabled()) LOG.debug("NameNode " + namenodeId + " is NOT alive, according to our records.");
    return false;
  }

  public long getLeCurrentId() {
    return this.getId();
  }

  /**
   * Return the current version of the active name nodes list.
   */
  public SortedActiveNodeList getActiveNameNodes() {
    return activeNameNodes;
  }

  /**
   * Return the current version of the active name nodes list.
   */
  public SortedActiveNodeList getActiveNameNodesWithRefresh() throws IOException {
    refreshActiveNameNodesList();

    return activeNameNodes;
  }

  /**
   * Return the ZooKeeper client.
   */
  public ZKClient getZooKeeperClient() {
    return zooKeeperClient;
  }

  /**
   * Return the current version of the active name nodes list.
   *
   * ClientProtocol.
   */
  private SortedActiveNodeList getActiveNamenodesForClient(TaskArguments fsArgs) {
    EntityManager.toggleMetadataCacheReads(true);
    return this.getActiveNameNodes();
  }

  /**
   * ClientProtocol.
   */
  private LocatedBlock updateBlockForPipeline(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);
    String clientName = fsArgs.getString(CLIENT_NAME); // fsArgs.getAsJsonPrimitive(CLIENT_NAME).getAsString();

    ExtendedBlock block = fsArgs.getObject("block");
//    if (fsArgs.contains("block")) {
////      String blockBase64 = fsArgs.getAsJsonPrimitive("block").getAsString();
////      block = (ExtendedBlock) InvokerUtilities.base64StringToObject(blockBase64);
//
//      block =
//    }

    return namesystem.updateBlockForPipeline(block, clientName);
  }

  /**
   * ClientProtocol.
   */
  private void updatePipeline(TaskArguments fsArgs) throws IOException, ClassNotFoundException {
    EntityManager.toggleMetadataCacheReads(false);
    String clientName = fsArgs.getString(CLIENT_NAME); // fsArgs.getAsJsonPrimitive(ServerlessNameNodeKeys.CLIENT_NAME).getAsString();

    ExtendedBlock oldBlock = null;
    if (fsArgs.contains("oldBlock")) {
      //String previousBase64 = fsArgs.getAsJsonPrimitive("oldBlock").getAsString();
      //oldBlock = (ExtendedBlock) InvokerUtilities.base64StringToObject(previousBase64);

      oldBlock = fsArgs.getObject("oldBlock");
    }

    ExtendedBlock newBlock = fsArgs.getObject("newBlock");
//    if (fsArgs.contains("newBlock")) {
//      //String previousBase64 = fsArgs.getAsJsonPrimitive("newBlock").getAsString();
//      //newBlock = (ExtendedBlock) InvokerUtilities.base64StringToObject(previousBase64);
//      newBlock = fsArgs.getObject("newBlock");
//    }

    DatanodeID[] newNodes = fsArgs.getObjectArray("newNodes"); //ServerlessUtilities.<DatanodeID>deserializeArgumentArray("newNodes", fsArgs);
//    if (fsArgs.contains("newNodes")) {
//      // Decode and deserialize the DatanodeInfo[].
//      JsonArray newNodesJsonArray = fsArgs.getAsJsonArray("newNodes");
//      newNodes = new DatanodeID[newNodesJsonArray.size()];
//
//      for (int i = 0; i < newNodesJsonArray.size(); i++) {
//        String newNodesBase64 = newNodesJsonArray.get(i).getAsString();
//        DatanodeID newNode = (DatanodeID) InvokerUtilities.base64StringToObject(newNodesBase64);
//        newNodes[i] = newNode;
//      }
//    }

    String[] newStorageIDs = fsArgs.getObject("newStorages");  //ServerlessUtilities.deserializeStringArray("newStorages", fsArgs);

    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs);
  }

  private void startMDCleanerService(){
    mdCleaner.startMDCleanerMonitor(namesystem, leaderElection, failedSTOCleanDelay, slowSTOCleanDelay);
  }

  private void createAndStartCRLFetcherService(Configuration conf) throws Exception {
    if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      if (conf.getBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY,
          CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_DEFAULT)) {
        LOG.info("Creating CertificateRevocationList Fetcher service");
        revocationListFetcherService = new RevocationListFetcherService();
        revocationListFetcherService.serviceInit(conf);
        revocationListFetcherService.serviceStart();
      } else {
        LOG.warn("RPC TLS is enabled but CRL validation is disabled");
      }
    }
  }

  /**
   * Returns whether the NameNode is completely started
   */
  boolean isStarted() {
    return this.started.get();
  }

  public BRTrackingService getBRTrackingService(){
    return brTrackingService;
  }

  @VisibleForTesting
  NameNodeRpcServer getNameNodeRpcServer(){
    return rpcServer;
  }

  static void createLeaseLocks(Configuration conf) throws IOException {
    int count = conf.getInt(DFSConfigKeys.DFS_LEASE_CREATION_LOCKS_COUNT_KEY,
            DFS_LEASE_CREATION_LOCKS_COUNT_DEFAULT);
    if (LOG.isDebugEnabled()) LOG.debug("Creating lease locks. Count = " + count + ".");
    new LightWeightRequestHandler(HDFSOperationType.CREATE_LEASE_LOCKS) {
      @Override
      public Object performTask() throws IOException {
        LeaseCreationLocksDataAccess da = (LeaseCreationLocksDataAccess) HdfsStorageFactory
                .getDataAccess(LeaseCreationLocksDataAccess.class);
        da.createLockRows(count);
        return null;
      }
    }.handle();
  }

  public static long getFailedSTOCleanDelay(){
    return failedSTOCleanDelay;
  }
}

