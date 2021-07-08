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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.leaderElection.HdfsLeDescriptorFactory;
import io.hops.leaderElection.LeaderElection;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.DataNodeDataAccess;
import io.hops.metadata.hdfs.dal.DatanodeStorageDataAccess;
import io.hops.metadata.hdfs.dal.LeaseCreationLocksDataAccess;
import io.hops.metadata.hdfs.dal.StorageReportDataAccess;
import io.hops.metadata.hdfs.entity.DataNodeMeta;
import io.hops.metadata.hdfs.entity.DatanodeStorage;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.security.HopsUGException;
import io.hops.security.UsersGroups;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.HadoopIllegalArgumentException;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingNonLeaderException;
import org.apache.hadoop.hdfs.server.blockmanagement.BRTrackingService;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
import static org.apache.hadoop.util.ExitUtil.terminate;

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
   * Indicates whether the initialization process has taken place yet. This will be true for warm function
   * containers, and in that sense it also serves as a flag indicating whether or not this is running
   * within a warm container or a new/cold container.
   */
  private static boolean initialized = false;

  /**
   * The single instance of the NameNode created by the serverless function.
   *
   * This is cached for future invocations.
   */
  private static ServerlessNameNode nameNodeInstance = null;

  /**
   * Added by Ben; mostly used for debugging (i.e., making sure the NameNode code that
   * is running is up-to-date with the source code base).
   *
   * Syntax:
   *  Major.Minor.Build.Revision
   */
  private static String versionNumber = "0.1.1.11";

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
          + RollingUpgradeStartupOption.getAllOptionString() + " ] | \n\t[" +
          //StartupOption.FINALIZE.getName() + "] | [" +
          //StartupOption.IMPORT.getName() + "] | [" +
          //StartupOption.INITIALIZESHAREDEDITS.getName() + "] | [" +
          //StartupOption.BOOTSTRAPSTANDBY.getName() + "] | [" +
          //StartupOption.RECOVER.getName() + " [ " +
          //StartupOption.FORCE.getName() + " ] ] | [ "+
          StartupOption.NO_OF_CONCURRENT_BLOCK_REPORTS.getName() + " concurrentBlockReports ] | [" +
          StartupOption.FORMAT_ALL.getName() + " ]";

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
  public static final Logger LOG =
      LoggerFactory.getLogger(ServerlessNameNode.class.getName());
  public static final Logger stateChangeLog =
      LoggerFactory.getLogger("org.apache.hadoop.hdfs.StateChange");
  public static final Logger blockStateChangeLog =
      LoggerFactory.getLogger("BlockStateChange");
 
  private static final String NAMENODE_HTRACE_PREFIX = "namenode.htrace.";
  
  protected FSNamesystem namesystem;
  protected final Configuration conf;
  private AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Identifies the NameNode. This is used in place of the leader election ID since leader election is not used
   * by serverless name nodes.
   *
   * This is set the first time this function is invoked (when it is warm, it should still be set...).
   *
   * The value is computed by hashing the activation ID of the OpenWhisk function.
   */
  private static long nameNodeID = -1;

  /**
   * Source: https://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
   * Used to convert the activation ID of this serverless function to a long to use as the NameNode ID.
   *
   * In theory, this is done only when the function is cold.
   */
  public static long hash(String string) {
    long h = 1125899906842597L; // prime
    int len = string.length();

    for (int i = 0; i < len; i++) {
      h = 31*h + string.charAt(i);
    }
    return h;
  }

  /**
   * Currently implemented for OpenWhisk.
   */
  private static void platformSpecificInitialization() {
    // TODO: Make this generic so that it can be readily reimplemented for arbitrary serverless platforms.
    String activationId = System.getenv("__OW_ACTIVATION_ID");

    LOG.info("System.getenv(\"HADOOP_CONF_DIR\") = " + System.getenv("HADOOP_CONF_DIR"));
    LOG.info("OpenWhisk activation ID = " + activationId);

    // OpenWhisk initialization.
    if (activationId != null)
      openWhiskInitialization(activationId);
    else
      ServerlessNameNode.nameNodeID = 1;
  }

  /**
   * Perform initialization specific to OpenWhisk.
   * @param activationId the activation ID of the running OpenWhisk function instance.
   */
  private static void openWhiskInitialization(String activationId) {
    if (ServerlessNameNode.nameNodeID == -1) {
      ServerlessNameNode.nameNodeID = hash(activationId);
      LOG.info("Set name node ID to " + ServerlessNameNode.nameNodeID);
    } else {
      LOG.info("Name node ID already set to " + ServerlessNameNode.nameNodeID);
    }
  }

  /**
   * OpenWhisk function handler. This is the main entrypoint for the serverless name node.
   */
  public static JsonObject main(JsonObject args) {
    LOG.info("=================================================================");
    LOG.info("Serverless NameNode v" + versionNumber + " has started executing.");
    LOG.info("=================================================================");
    System.setProperty("sun.io.serialization.extendedDebugInfo", "true");

    // The arguments passed by the user are included under the 'value' key.
    JsonObject userArguments = args.get("value").getAsJsonObject();

    LOG.debug("Top-level OpenWhisk arguments = " + args.toString());
    LOG.debug("User-passed OpenWhisk arguments = " + userArguments.toString());

    platformSpecificInitialization();

    String[] commandLineArguments;

    // Attempt to extract the command-line arguments, which will be passed as a single string parameter.
    if (userArguments.has("command-line-arguments"))
      commandLineArguments = userArguments.getAsJsonPrimitive("command-line-arguments").getAsString().split("\\s+");
    else
      commandLineArguments = new String[0];

    String op = null;
    JsonObject fsArgs = null;

    if (userArguments.has("op"))
      op = userArguments.getAsJsonPrimitive("op").getAsString();

    // JSON dictionary containing the arguments/parameters for the specified filesystem operation.
    if (userArguments.has("fsArgs"))
      fsArgs = userArguments.getAsJsonObject("fsArgs");

    JsonObject result = nameNodeDriver(op, fsArgs, commandLineArguments);
    LOG.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
    LOG.debug("Result to be returned to the caller:\n" + result);
    LOG.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");

    // Resource: https://medium.com/openwhisk/web-actions-serverless-web-apps-with-openwhisk-f21db459f9ba
    // Resource: https://github.com/apache/openwhisk/blob/master/docs/webactions.md

    JsonObject response = new JsonObject();

    JsonObject headers = new JsonObject();
    headers.addProperty("content-type", "application/json");

    response.addProperty("statusCode", 200);
    response.add("headers", headers);
    response.add("body", result);

    return response;
  }

  /**
   * This is called by both main methods.
   */
  private static JsonObject nameNodeDriver(String op, JsonObject fsArgs, String[] commandLineArguments) {
    JsonObject response = new JsonObject();

    if (LOG.isDebugEnabled())
      LOG.info("Debug-logging IS enabled.");
    else
      LOG.info("Debug-logging is NOT enabled.");

    LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
    LOG.info("NameNode Argument Information:");
    LOG.info("Op = " + op);
    if (fsArgs != null)
      LOG.info("fsArgs = " + fsArgs.toString());
    else
      LOG.info("fsArgs = NULL");
    LOG.info("commandLineArguments = " + Arrays.toString(commandLineArguments));
    LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");

    // Check if we need to initialize the namenode.
    if (!initialized) {
      try {
        LOG.debug("This is a COLD START. Creating the NameNode now...");
        nameNodeInstance = startServerlessNameNode(commandLineArguments);
      } catch (Exception ex) {
        LOG.error("Encountered exception while initializing the name node.", ex);
        response.addProperty("EXCEPTION", ex.toString());
      }
    }
    else
      LOG.debug("NameNode is already initialized. Skipping initialization step.");

    if (nameNodeInstance == null) {
      LOG.error("NameNodeInstance is null despite having been initialized.");
      response.addProperty("ERROR-MESSAGE", "Failed to initialize NameNode. Unknown error. Review logs for details.");
      return response;
    }

    try {
      List<DatanodeRegistration> datanodeRegistrations = nameNodeInstance.getDataNodesFromIntermediateStorage();

      // TODO: Retrieve storage reports.

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
      for (DatanodeRegistration registration : datanodeRegistrations) {
        String datanodeUuid = registration.getDatanodeUuid();

        LOG.info("Retrieving DatanodeRegistration instances for datanode " + datanodeUuid);
        HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> datanodeStorageMap
                = nameNodeInstance.retrieveAndConvertDatanodeStorages(registration);

        datanodeStorageMaps.put(datanodeUuid, datanodeStorageMap);
      }

      HashMap<String, List<io.hops.metadata.hdfs.entity.StorageReport>> storageReportMap
              = nameNodeInstance.retrieveStorageReports(datanodeRegistrations);

      HashMap<String, List<org.apache.hadoop.hdfs.server.protocol.StorageReport>> convertedStorageReportMap
              = new HashMap<>();

      // Iterate over all of the storage reports. The keys are datanodeUuids and the values are storage reports.
      for (Map.Entry<String, List<StorageReport>> entry : storageReportMap.entrySet()) {
        String datanodeUuid = entry.getKey();
        List<StorageReport> storageReports = entry.getValue();
        LOG.debug("Storage Reports for Data Node: " + datanodeUuid);

        // Get the mapping of storageIds to DatanodeStorage instances for this particular datanode.
        HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> datanodeStorageMap
                = datanodeStorageMaps.get(datanodeUuid);

        ArrayList<org.apache.hadoop.hdfs.server.protocol.StorageReport> convertedStorageReports =
                new ArrayList<>();

        // For each storage report associated with the current datanode, convert it to a HopsFS storage report (they
        // are currently the DAL storage reports, which are just designed to be used with intermediate storage).
        for (StorageReport report : storageReports) {
          LOG.debug(report.toString());

          org.apache.hadoop.hdfs.server.protocol.StorageReport convertedReport
                  = new org.apache.hadoop.hdfs.server.protocol.StorageReport(
                          datanodeStorageMap.get(report.getDatanodeStorageId()), report.getFailed(),
                    report.getCapacity(), report.getDfsUsed(), report.getRemaining(), report.getBlockPoolUsed());

          convertedStorageReports.add(convertedReport);
        }

        convertedStorageReportMap.put(datanodeUuid, convertedStorageReports);
      }

      LOG.debug("Processing storage reports from " + convertedStorageReportMap.size() + " data nodes now...");

      for (DatanodeRegistration registration : datanodeRegistrations) {

        // For each registration, we call the `handleServerlessStorageReports()` function. We pass the given
        // registration, then we trieve the list of storage reports from the mapping, convert it to an Object[],
        // and cast it to an array of HopsFS StorageReport[].
        nameNodeInstance.namesystem.handleServerlessStorageReports(
                registration, (org.apache.hadoop.hdfs.server.protocol.StorageReport[]) convertedStorageReportMap.get(
                        registration.getDatanodeUuid()).toArray());
      }

      JsonObject result = nameNodeInstance.performOperation(op, fsArgs);

      response.add("RESULT", result);
    }
    catch (Exception ex) {
      LOG.error("Exception encountered during execution of Serverless NameNode.");
      ex.printStackTrace();
      response.addProperty("EXCEPTION", ex.toString());
    }

    return response;
  }

  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }

  public JsonObject performOperation(String op, JsonObject fsArgs) throws IOException, ClassNotFoundException {
    LOG.info("Specified operation: " + op);

    if (op == null) {
      LOG.info("User did not specify an operation.");
      return new JsonObject(); // empty
    }

    Object returnValue = null;
    JsonObject result = null;

    // Now we perform the desired/specified operation.
    switch(op) {
      case "addBlock":
        returnValue = addBlockOperation(fsArgs);
        break;
      case "append":
        appendOperation(fsArgs);
        break;
      case "complete":
        break;
      case "concat":
        concatOperation(fsArgs);
        break;
      case "create":
        returnValue = createOperation(fsArgs);
        break;
      case "delete":
        deleteOperation(fsArgs);
        break;
      case "rename":
        renameOperation(fsArgs);
        break;
      case "versionRequest":
        returnValue = versionRequestOperation(fsArgs);
        break;
      default:
        LOG.info("Unknown operation: " + op);
    }

    // Serialize the resulting HdfsFileStatus/LocatedBlock/etc. object, if it exists, and encode it to Base64 so we
    // can include it in the JSON response sent back to the invoker of this serverless function.
    if (returnValue != null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = null;

      try {
        LOG.info("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-");
        LOG.info("returnValue.getClass() = " + returnValue.getClass());
        LOG.info("returnValue instanceof Serializable: " + (returnValue instanceof Serializable));
        LOG.info("returnValue BEFORE being serialized:\n" + returnValue.toString());
        LOG.info("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-");
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(returnValue);
        objectOutputStream.flush();

        byte[] objectBytes = byteArrayOutputStream.toByteArray();
        String base64Object = Base64.encodeBase64String(objectBytes);

        result = new JsonObject();
        result.addProperty("base64result", base64Object);
      } catch (Exception ex) {
        LOG.error("Exception encountered whilst serializing result of file system operation.");
        ex.printStackTrace();
        result = new JsonObject();
        result.addProperty("EXCEPTION", ex.toString());
      } finally {
        try {
          byteArrayOutputStream.close();
        } catch (IOException ex) {
          // Ignore close exception.
        }
      }
    }

    // Create this object if it wasn't already created.
    if (result == null)
      result = new JsonObject(); // empty

    return result;
  }

  /**
   * Used as the entry-point into the Serverless NameNode when executing a serverless function.
   *
   * @param commandLineArgs Command-line arguments formatted as if the NameNode was being executed from the commandline.
   * @throws Exception
   */
  public static ServerlessNameNode startServerlessNameNode(String[] commandLineArgs) throws Exception {
    if (DFSUtil.parseHelpArgument(commandLineArgs, ServerlessNameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    LOG.info("Creating and initializing Serverless NameNode now...");

    try {
      StringUtils.startupShutdownMessage(ServerlessNameNode.class, commandLineArgs, LOG);
      ServerlessNameNode nameNode = createNameNode(commandLineArgs, null);

      if (nameNode == null) {
        LOG.info("ERROR: NameNode is null. Failed to create and/or initialize the Serverless NameNode.");
        terminate(1);
      } else {
        LOG.info("Successfully created and initialized Serverless NameNode.");
      }

      initialized = true;
      return nameNode;
    } catch (Throwable e) {
      LOG.error("Failed to start namenode.", e);
      terminate(1, e);
    }

    initialized = false;
    return null;
  }

  /**
   * Retrieve the DatanodeStorage instances stored in intermediate storage.
   * These are used in conjunction with StorageReports.
   *
   * This method will convert the objects from their DAL versions to the HopsFS versions.
   */
  private HashMap<String, org.apache.hadoop.hdfs.server.protocol.DatanodeStorage> retrieveAndConvertDatanodeStorages(
          DatanodeRegistration datanodeRegistration) throws IOException {
    LOG.info("Retrieving DatanodeStorage instances from intermediate storage now...");

    DatanodeStorageDataAccess<DatanodeStorage> dataAccess =
            (DatanodeStorageDataAccess)HdfsStorageFactory.getDataAccess(DatanodeStorageDataAccess.class);

    List<DatanodeStorage> datanodeStorages = dataAccess.getDatanodeStorages(datanodeRegistration.getDatanodeUuid());

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
          List<DatanodeRegistration> registrations) throws IOException {
    LOG.info("Retrieving StorageReport instances for " + registrations.size() + " data nodes now...");

    StorageReportDataAccess<io.hops.metadata.hdfs.entity.StorageReport> dataAccess =
            (StorageReportDataAccess)HdfsStorageFactory.getDataAccess(StorageReportDataAccess.class);

    HashMap<String, List<io.hops.metadata.hdfs.entity.StorageReport>> storageReportMap = new HashMap<>();

    for (DatanodeRegistration registration : registrations) {
      List<io.hops.metadata.hdfs.entity.StorageReport> storageReports = retrieveStorageReports(registration);
      storageReportMap.put(registration.getDatanodeUuid(), storageReports);
    }

    return storageReportMap;
  }

  /**
   * Retrieve the storage reports associated with one particular DataNode.
   * @param registration
   * @return
   * @throws IOException
   */
  private List<io.hops.metadata.hdfs.entity.StorageReport> retrieveStorageReports(DatanodeRegistration registration)
          throws IOException {
    LOG.info("Retrieving StorageReport instance for datanode " + registration.getDatanodeUuid());

    StorageReportDataAccess<io.hops.metadata.hdfs.entity.StorageReport> dataAccess =
            (StorageReportDataAccess)HdfsStorageFactory.getDataAccess(StorageReportDataAccess.class);

    List<io.hops.metadata.hdfs.entity.StorageReport> storageReports
            = dataAccess.getLatestStorageReports(registration.getDatanodeUuid());

    return storageReports;
  }

  /**
   * Retrieve the DataNodes from intermediate storage.
   *
   * @return List of DatanodeRegistration instances to be used to retrieve serverless storage reports once
   * the registration step(s) have been completed.
   */
  private List<DatanodeRegistration> getDataNodesFromIntermediateStorage() throws IOException {
    // Retrieve the DataNodes from intermediate storage.
    LOG.info("Retrieving list of DataNodes from intermediate storage now...");
    DataNodeDataAccess<DataNodeMeta> dataAccess = (DataNodeDataAccess)
            HdfsStorageFactory.getDataAccess(DataNodeDataAccess.class);
    List<DataNodeMeta> dataNodes = dataAccess.getAllDataNodes();
    LOG.info("Retrieved list of DataNodes from intermediate storage with " + dataNodes.size() + " entries!");

    NamespaceInfo nsInfo = namesystem.getNamespaceInfo();

    // Keep track of the DatanodeRegistration instances because we'll need these to retrieve
    // the storage reports from intermediate storage after we've registered the data node(s).
    List<DatanodeRegistration> datanodeRegistrations = new ArrayList<>();

    for (DataNodeMeta dataNodeMeta : dataNodes) {
      LOG.info("Discovered " + dataNodeMeta.toString());
      LOG.info("IP Address: {}, Hostname: {}, Xfer Port: {}, Info Port: {}, Info Secure Port: {}, IPC Port: {}",
              dataNodeMeta.getIpAddress(), dataNodeMeta.getHostname(), dataNodeMeta.getXferPort(),
              dataNodeMeta.getInfoPort(), dataNodeMeta.getInfoSecurePort(), dataNodeMeta.getIpcPort());

      DatanodeID dnId =
          new DatanodeID(dataNodeMeta.getIpAddress(), dataNodeMeta.getHostname(),
              dataNodeMeta.getDatanodeUuid(), dataNodeMeta.getXferPort(), dataNodeMeta.getInfoPort(),
                  dataNodeMeta.getInfoSecurePort(), dataNodeMeta.getIpcPort());

      StorageInfo storageInfo = new StorageInfo(
              DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
              nsInfo.getNamespaceID(), nsInfo.clusterID, nsInfo.getCTime(),
              HdfsServerConstants.NodeType.DATA_NODE, nsInfo.getBlockPoolID());

      LOG.info("NamespaceID: {}, ClusterID: {}, CTime: {}, BlockPoolID: {}", nsInfo.getNamespaceID(),
              nsInfo.clusterID, nsInfo.getCTime(), nsInfo.getBlockPoolID());

      DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
              dnId, storageInfo, new ExportedBlockKeys(), VersionInfo.getVersion());

      try {
        namesystem.registerDatanode(datanodeRegistration);

        datanodeRegistrations.add(datanodeRegistration);
      } catch (IOException ex) {
        // Log this so we know the source of the exception, then re-throw it so it gets caught one layer up.
        LOG.error("Error registering datanode " + dataNodeMeta.getDatanodeUuid());
        throw ex;
      }
    }

    return datanodeRegistrations;
  }

  private LocatedBlock addBlockOperation(JsonObject fsArgs) throws IOException, ClassNotFoundException {
    String src = fsArgs.getAsJsonPrimitive("src").getAsString();
    String clientName = fsArgs.getAsJsonPrimitive("clientName").getAsString();

    long fileId = fsArgs.getAsJsonPrimitive("fileId").getAsLong();

    String[] favoredNodes = null;

    if (fsArgs.has("favoredNodes")) {
      JsonArray favoredNodesJsonArray = fsArgs.getAsJsonArray("favoredNodes");
      favoredNodes = new String[favoredNodesJsonArray.size()];

      for (int i = 0; i < favoredNodesJsonArray.size(); i++) {
        favoredNodes[i] = favoredNodesJsonArray.get(i).getAsString();
      }
    }

    ExtendedBlock previous = null;

    if (fsArgs.has("previous")) {
      String previousBase64 = fsArgs.getAsJsonPrimitive("previous").getAsString();
      byte[] previousBytes = Base64.decodeBase64(previousBase64);
      DataInputBuffer dataInput = new DataInputBuffer();
      dataInput.reset(previousBytes, previousBytes.length);
      previous = (ExtendedBlock) ObjectWritable.readObject(dataInput, null);
    }

    /*boolean blockIncluded = fsArgs.getAsJsonPrimitive("blockIncluded").getAsBoolean();

    if (blockIncluded) {
      LOG.info("Block variable WAS included in payload. Extracting now...");

      // First the block poolID (the order we extract doesn't matter).
      String blockPoolId = fsArgs.getAsJsonPrimitive("block.poolId").getAsString();

      // Decode and deserialize the block.
      String blockBase64 = fsArgs.getAsJsonPrimitive("blockBase64").getAsString();
      byte[] blockBytes = Base64.decodeBase64(blockBase64);
      DataInputBuffer dataInput = new DataInputBuffer();
      dataInput.reset(blockBytes, blockBytes.length);
      Block block = (Block) ObjectWritable.readObject(dataInput, null);
      previous = new ExtendedBlock(blockPoolId, block);
    } else {
      LOG.info("Block variable was NOT included in payload.");
    }*/

    DatanodeInfo[] excludeNodes = null;
    if (fsArgs.has("excludeNodes")) {
      // Decode and deserialize the DatanodeInfo[].
      JsonArray excludedNodesJsonArray = fsArgs.getAsJsonArray("favoredNodes");
      excludeNodes = new DatanodeInfo[excludedNodesJsonArray.size()];

      for (int i = 0; i < excludedNodesJsonArray.size(); i++) {
        String excludedNodeBase64 = excludedNodesJsonArray.get(i).getAsString();
        byte[] excludedNodeBytes = Base64.decodeBase64(excludedNodeBase64);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(excludedNodeBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        DatanodeInfo excludedNode = (DatanodeInfo)objectInputStream.readObject();
        excludeNodes[i] = excludedNode;
      }
    }

    LOG.info("addBlock() function of ServerlessNameNodeRpcServer called.");
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
              "*BLOCK* NameNode.addBlock: file " + src + " fileId=" + fileId + " for " + clientName);
    }
    HashSet<Node> excludedNodesSet = null;

    if (excludeNodes != null) {
      excludedNodesSet = new HashSet<>(excludeNodes.length);
      excludedNodesSet.addAll(Arrays.asList(excludeNodes));
    }
    List<String> favoredNodesList = null;
    if (favoredNodes != null)
      favoredNodesList = Arrays.asList(favoredNodes);

    return namesystem.getAdditionalBlock(src, fileId, clientName, previous, excludedNodesSet, favoredNodesList);
  }

  private void appendOperation(JsonObject fsArgs) {
    throw new NotImplementedException("Operation `append` has not been implemented yet.");
  }

  private boolean completeOperation(JsonObject fsArgs) throws IOException {
    String src = fsArgs.getAsJsonPrimitive("src").getAsString();
    String clientName = fsArgs.getAsJsonPrimitive("clientName").getAsString();

    long fileId = fsArgs.getAsJsonPrimitive("fileId").getAsLong();

    ExtendedBlock last = null;

    // TODO: Add helper/utility functions to reduce boilerplate code when extracting arguments.
    //       References:
    //       - https://stackoverflow.com/questions/11664894/jackson-deserialize-using-generic-class
    //       - https://stackoverflow.com/questions/11659844/jackson-deserialize-generic-class-variable
    //       - https://stackoverflow.com/questions/17400850/is-jackson-really-unable-to-deserialize-json-into-a-generic-type
    if (fsArgs.has("last")) {
      String lastBase64 = fsArgs.getAsJsonPrimitive("last").getAsString();
      byte[] lastBytes = Base64.decodeBase64(lastBase64);
      DataInputBuffer dataInput = new DataInputBuffer();
      dataInput.reset(lastBytes, lastBytes.length);
      last = (ExtendedBlock) ObjectWritable.readObject(dataInput, null);
    }

    byte[] data = null;

    if (fsArgs.has("data")) {
      String dataBase64 = fsArgs.getAsJsonPrimitive("data").getAsString();
      data = Base64.decodeBase64(dataBase64);
    }

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: " + src + " fileId=" + fileId +" for " + clientName);
    }

    return namesystem.completeFile(src, clientName, last, fileId, data);
  }

  private void concatOperation(JsonObject fsArgs) {
    throw new NotImplementedException("Operation `concat` has not been implemented yet.");
  }

  private NamespaceInfo versionRequestOperation(JsonObject fsArgs) throws IOException {
    LOG.info("Performing versionRequest operation now...");
    namesystem.checkSuperuserPrivilege();
    return namesystem.getNamespaceInfo();
  }

  /**
   (String src, FsPermission masked,
   String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
   short replication, long blockSize, CryptoProtocolVersion[] supportedVersions, EncodingPolicy policy)
   */

  private HdfsFileStatus createOperation(JsonObject fsArgs) throws IOException {
    LOG.info("Unpacking arguments for the CREATE operation now...");

    String src = fsArgs.getAsJsonPrimitive("src").getAsString();
    short permissionAsShort = fsArgs.getAsJsonPrimitive("masked").getAsShort();
    FsPermission masked = new FsPermission(permissionAsShort);
    String clientName = fsArgs.getAsJsonPrimitive("clientName").getAsString();

    byte[] enumSetSerialized = Base64.decodeBase64(fsArgs.getAsJsonPrimitive("enumSetBase64").getAsString());

    DataInputBuffer dataInput = new DataInputBuffer();
    dataInput.reset(enumSetSerialized, enumSetSerialized.length);
        /*ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(enumSetSerialized);
        DataInput dataInput = new ObjectInputStream(byteArrayInputStream);*/
    EnumSet<CreateFlag> flag = ((EnumSetWritable<CreateFlag>) ObjectWritable.readObject(dataInput, null)).get();

    boolean createParent = fsArgs.getAsJsonPrimitive("createParent").getAsBoolean();
    short replication = fsArgs.getAsJsonPrimitive("replication").getAsShort();
    long blockSize = fsArgs.getAsJsonPrimitive("blockSize").getAsLong();
    CryptoProtocolVersion[] supportedVersions = CryptoProtocolVersion.supported();

    EncodingPolicy policy = null;
    boolean policyExists = fsArgs.getAsJsonPrimitive("policyExists").getAsBoolean();
    if (policyExists) {
      String codec = fsArgs.getAsJsonPrimitive("codec").getAsString();
      short targetReplication = fsArgs.getAsJsonPrimitive("targetReplication").getAsShort();
      policy = new EncodingPolicy(codec, targetReplication);
    }

    LOG.info("Create Arguments:\n\tsrc = " + src + "\n\tclientName = "+ clientName + "\n\tcreateParent = " +
            createParent + "\n\treplication = " + replication + "\n\tblockSize = " + blockSize);

    if (!checkPathLength(src)) {
      throw new IOException(
              "create: Pathname too long.  Limit " + MAX_PATH_LENGTH +
                      " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    // I don't know what to use for this; the RPC server has a method for it, but I don't know if it applies to serverless case...
    String clientMachine = "";

    HdfsFileStatus stat = namesystem.startFile(
            src, new PermissionStatus(getRemoteUser().getShortUserName(), null, masked),
            clientName, clientMachine, flag, createParent, replication, blockSize, supportedVersions);

    // Currently impossible to pass null for EncodingPolicy, but pretending it's possible for now...
    if (policy != null) {
      if (!namesystem.isErasureCodingEnabled()) {
        throw new IOException("Requesting encoding although erasure coding" +
                " was disabled");
      }
      LOG.info("Create file " + src + " with policy " + policy.toString());
      namesystem.addEncodingStatus(src, policy,
              EncodingStatus.Status.ENCODING_REQUESTED, false);
    }

    return stat;
  }

  private void deleteOperation(JsonObject fsArgs) {

  }

  private void renameOperation(JsonObject fsArgs) {

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

  public static void main(String[] args) throws Exception {
    LOG.info("=================================================================");
    LOG.info("Serverless NameNode v" + versionNumber + " has started executing.");
    LOG.info("=================================================================");
    System.setProperty("sun.io.serialization.extendedDebugInfo", "true");

    LOG.debug("JsonObject args = " + args.toString());

    platformSpecificInitialization();

    CommandLine cmd = parseMainArguments(args);

    String[] commandLineArguments;

    // Attempt to extract the command-line arguments, which will be passed as a single string parameter.
    if (cmd.hasOption("command-line-args"))
      commandLineArguments = new String[]{cmd.getOptionValue("command-line-args")};
    else
      commandLineArguments = new String[0];

    String op = null;
    JsonObject fsArgs = null;

    if (cmd.hasOption("op"))
      op = cmd.getOptionValue("op");

    // JSON dictionary containing the arguments/parameters for the specified filesystem operation.
    if (cmd.hasOption("fsArgs")) {
      String fsArgsAsString = cmd.getOptionValue("fsArgs");
      JsonParser parser = new JsonParser();
      fsArgs = parser.parse(fsArgsAsString).getAsJsonObject(); // Convert to JsonObject.
    }

    JsonObject response = nameNodeDriver(op, fsArgs, commandLineArguments);
    LOG.info("Response = " + response);
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

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = FSNamesystem.loadFromDisk(conf, this);
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
  protected void initialize(Configuration conf) throws IOException {
   if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
      String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
      if (intervals != null) {
        conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
          intervals);
      }
    }
    
    UserGroupInformation.setConfiguration(conf);
    loginAsNameNodeUser(conf);

    //LOG.debug("Setting up the configuration for the HdfsStorageFactory now...");

    //LOG.debug(HdfsStorageFactory.class.getSimpleName() + ".class");
    //LOG.debug(String.valueOf(HdfsStorageFactory.class.getResource("HdfsStorageFactory.class")));

    ClassLoader loader = HdfsStorageFactory.class.getClassLoader();
    //LOG.debug(String.valueOf(loader.getResource("io/hops/metadata/HdfsStorageFactory.class")));
    //LOG.debug(String.valueOf(HdfsStorageFactory.class.getProtectionDomain().getCodeSource().getLocation()));
    
    HdfsStorageFactory.setConfiguration(conf);

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
    this.failedSTOCleanDelay = conf.getLong(
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

    // Serverless NameNodes do not need to start the HTTP server.
    // startHttpServer(conf);
    loadNamesystem(conf);

    // Serverless NameNodes do not need to create or start an RPC server.
    // rpcServer = createRpcServer(conf);

    //tokenServiceName = NetUtils.getHostPortString(rpcServer.getRpcAddress());
    //httpServer.setNameNodeAddress(getNameNodeAddress());

    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(conf);
    pauseMonitor.start();

    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    
    startCommonServices(conf);

    if(isLeader()){ //if the newly started namenode is the leader then it means
      //that is cluster was restarted and we can reset the number of default
      // concurrent block reports
      HdfsVariables.setMaxConcurrentBrs(maxConcurrentBRs, null);
      createLeaseLocks(conf);
    }

    // in case of cluster upgrade the retry cache epoch is set to 0
    // update the epoch to correct value
    if (HdfsVariables.getRetryCacheCleanerEpoch() == 0){
      // -1 to ensure the entries in the current epoch are delete by the cleaner
      HdfsVariables.setRetryCacheCleanerEpoch(System.currentTimeMillis()/1000 - 1);
    }
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
   */
  private void startCommonServices(Configuration conf) throws IOException {
    LOG.debug("=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
    LOG.debug("Starting common NameNode services now...");
    LOG.debug("=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
    LOG.debug("NOT starting the Leader Election Service.");
    // startLeaderElectionService();

    startMDCleanerService();
    
    namesystem.startCommonServices(conf);
    registerNNSMXBean();

    LOG.debug("NOT starting the RPC server.");
    // rpcServer.start();
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
  public ServerlessNameNode(Configuration conf) throws IOException {
    this(conf, NamenodeRole.NAMENODE);
  }

  protected ServerlessNameNode(Configuration conf, NamenodeRole role) throws IOException {
    this.tracer = new Tracer.Builder("NameNode").
      conf(TraceUtils.wrapHadoopConf(NAMENODE_HTRACE_PREFIX, conf)).
      build();
    this.tracerConfigurationManager =
      new TracerConfigurationManager(NAMENODE_HTRACE_PREFIX, conf);
    this.conf = conf;
    try {
      initializeGenericKeys(conf);
      initialize(conf);
      this.started.set(true);
      enterActiveState();
    } catch (IOException | HadoopIllegalArgumentException e) {
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


  public static ServerlessNameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    LOG.info("createNameNode " + Arrays.asList(argv));
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      return null;
    }
    setStartupOption(conf, startOpt);

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
        DefaultMetricsSystem.initialize("NameNode");
        return new ServerlessNameNode(conf);
      }
    }
  }

  public static void initializeGenericKeys(Configuration conf) {
    // If the RPC address is set use it to (re-)configure the default FS
    if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" +
          conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting " + FS_DEFAULT_NAME_KEY + " to " + defaultUri.toString());
      }
    }
  }

  /** 
   */
  /*public static void main(String argv[]) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, ServerlessNameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(ServerlessNameNode.class, argv, LOG);
      ServerlessNameNode namenode = createNameNode(argv, null);
      if (namenode != null) {
        namenode.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start namenode.", e);
      terminate(1, e);
    }
  }*/

  private void enterActiveState() throws ServiceFailedException {
    try {
      startActiveServicesInternal();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start active services", e);
    }
  }

  private void startActiveServicesInternal() throws IOException {
    try {
      namesystem.startActiveServices();
      startTrashEmptier(conf);
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
    terminate(1, t);
  }

  /**
   * Returns the id of this namenode
   */
  public long getId() {
    //return leaderElection.getCurrentId();
    return ServerlessNameNode.nameNodeID;
  }

  /**
   * Return the {@link LeaderElection} object.
   *
   * @return {@link LeaderElection} object.
   */
  public LeaderElection getLeaderElectionInstance() {
    return leaderElection;
  }

  public boolean isLeader() {
    if (leaderElection != null) {
      return leaderElection.isLeader();
    } else {
      return false;
    }
  }

  public ActiveNode getNextNamenodeToSendBlockReport(final long noOfBlks, DatanodeID nodeID) throws IOException {
    if (leaderElection.isLeader()) {
      DatanodeDescriptor node = namesystem.getBlockManager().getDatanodeManager().getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        throw new IOException(
            "ProcessReport from dead or unregistered node: " + nodeID+ ". "
                    + (node != null ? ("The node is alive : " + node.isAlive) : "The node is null "));
      }
      LOG.debug("NN Id: " + leaderElection.getCurrentId() + ") Received request to assign" +
              " block report work ("+ noOfBlks + " blks) ");
      ActiveNode an = brTrackingService.assignWork(leaderElection.getActiveNamenodes(),
              nodeID.getXferAddr(), noOfBlks);
      return an;
    } else {
      String msg = "NN Id: " + leaderElection.getCurrentId() + ") Received request to assign" +
              " work (" + noOfBlks + " blks). Returning null as I am not the leader NN";
      LOG.debug(msg);
      throw new BRLoadBalancingNonLeaderException(msg);
    }
  }

  public static boolean isNameNodeAlive(Collection<ActiveNode> activeNamenodes,
      long namenodeId) {
    if (activeNamenodes == null) {
      // We do not know yet, be conservative
      return true;
    }

    for (ActiveNode namenode : activeNamenodes) {
      if (namenode.getId() == namenodeId) {
        return true;
      }
    }
    return false;
  }

  public long getLeCurrentId() {
    LOG.warn("LeaderElection ID has been replaced by standard NameNode ID because LeaderElection is disabled!");
    return this.getId();
    // return leaderElection.getCurrentId();
  }

  public SortedActiveNodeList getActiveNameNodes() {
    //return leaderElection.getActiveNamenodes();

    return new SortedActiveNodeList() {

      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public int size() {
        return 0;
      }

      @Override
      public List<ActiveNode> getActiveNodes() {
        return new ArrayList<ActiveNode>();
      }

      @Override
      public List<ActiveNode> getSortedActiveNodes() {
        return new ArrayList<ActiveNode>();
      }

      @Override
      public ActiveNode getActiveNode(InetSocketAddress address) {
        return null;
      }

      @Override
      public ActiveNode getLeader() {
        return null;
      }
    };
  }

  private void startMDCleanerService(){
    mdCleaner.startMDCleanerMonitor(namesystem, leaderElection, failedSTOCleanDelay, slowSTOCleanDelay);
  }

  private void stopMDCleanerService(){
    mdCleaner.stopMDCleanerMonitor();
  }

  private void startLeaderElectionService() throws IOException {
    // Initialize the leader election algorithm (only once rpc server is
    // created and httpserver is started)
    long leadercheckInterval =
        conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
            DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
        conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
            DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    int leIncrement = conf.getInt(DFSConfigKeys.DFS_LEADER_TP_INCREMENT_KEY,
        DFSConfigKeys.DFS_LEADER_TP_INCREMENT_DEFAULT);

    String rpcAddresses = "";
    rpcAddresses = rpcServer.getRpcAddress().getAddress().getHostAddress() + ":" +rpcServer.getRpcAddress().getPort()+",";
    if(rpcServer.getServiceRpcAddress() != null){
      rpcAddresses = rpcAddresses + rpcServer.getServiceRpcAddress().getAddress().getHostAddress() + ":" +
              rpcServer.getServiceRpcAddress().getPort();
    }

    String httpAddress;
    /*
     * httpServer.getHttpAddress() return the bind address. If we use 0.0.0.0 to listen to all interfaces the leader
     * election system will return 0.0.0.0 as the http address and the client will not be able to connect to the UI
     * to mitigate this we retunr the address used by the RPC. This address will work because the http server is 
     * listening on very interfaces
     * */
    
    if (DFSUtil.getHttpPolicy(conf).isHttpEnabled()) {
      if (httpServer.getHttpAddress().getAddress().getHostAddress().equals("0.0.0.0")) {
        httpAddress = rpcServer.getRpcAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpAddress()
            .getPort();
      } else {
        httpAddress = httpServer.getHttpAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpAddress()
            .getPort();
      }
    } else {
      if (httpServer.getHttpsAddress().getAddress().getHostAddress().equals("0.0.0.0")) {
        httpAddress = rpcServer.getRpcAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpsAddress()
            .getPort();
      } else {
        httpAddress = httpServer.getHttpsAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpsAddress()
            .getPort();
      }
    }
    
    leaderElection =
        new LeaderElection(new HdfsLeDescriptorFactory(), leadercheckInterval,
            missedHeartBeatThreshold, leIncrement, httpAddress,
            rpcAddresses, (byte) conf.getInt(DFSConfigKeys.DFS_LOCATION_DOMAIN_ID,
            DFSConfigKeys.DFS_LOCATION_DOMAIN_ID_DEFAULT));
    leaderElection.start();

    try {
      leaderElection.waitActive();
    } catch (InterruptedException e) {
      LOG.warn("NN was interrupted");
    }
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

