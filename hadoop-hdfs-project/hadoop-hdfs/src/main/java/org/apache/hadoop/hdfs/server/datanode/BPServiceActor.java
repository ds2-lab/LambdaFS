/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.ByteArrayInputStream;
import java.io.EOFException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import javax.xml.stream.events.Namespace;

/**
 * A thread per active or standby namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 */
@InterfaceAudience.Private
class BPServiceActor implements Runnable {

  static final Log LOG = DataNode.LOG;
  final InetSocketAddress nnAddr;
  BPOfferService bpos;
  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;
  private final Scheduler scheduler;
  
  static enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
  }
  private volatile RunningState runningState = RunningState.CONNECTING;
  
  private volatile boolean shouldServiceRun = true;
  private final DataNode dn;
  private final DNConf dnConf;

  private DatanodeRegistration bpRegistration;

  private boolean connectedToNN = false;

  BPServiceActor(InetSocketAddress nnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
    scheduler = new Scheduler(dnConf.heartBeatInterval);
  }

  boolean isRunning(){
    if(!isAlive()){
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING;
  }
  
  boolean isAlive() {
    if (!shouldServiceRun || !bpThread.isAlive()) {
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING
        || runningState == BPServiceActor.RunningState.CONNECTING;
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }

  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   *
   * @return the NamespaceInfo
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        // Instead of using RPC to call the create() function, we perform a serverless invocation.
        String uri = dnConf.serverlessEndpoint;
        LOG.info("OpenWhisk URI: \"" + uri + "\"");
        HttpPost request = new HttpPost(uri);
        request.addHeader("content-type", "application/json");

        // Arguments for the NameNode program itself.
        JsonObject namenodeArgs = new JsonObject();

        // Arguments for the particular operation we're performing.
        JsonObject opArguments = new JsonObject();

        // Add the function arguments to the invocation request arguments.
        namenodeArgs.add("fsArgs", opArguments);
        namenodeArgs.addProperty("op", "versionRequest");
        namenodeArgs.addProperty("command-line-arguments", "-regular");

        JsonObject requestArguments = new JsonObject();
        // It looks like OpenWhisk expects the arguments for the serverless
        // function to be stored with the key/property "value".
        requestArguments.add("value", namenodeArgs);

        StringEntity params = new StringEntity(requestArguments.toString());
        request.setEntity(params);

        HttpResponse response = httpClient.execute(request);

        String json = EntityUtils.toString(response.getEntity(), "UTF-8");
        //LOG.info("json = " + json);
        Gson gson = new Gson();
        JsonObject responseJson = gson.fromJson(json, JsonObject.class);

        LOG.info("responseJson = " + responseJson.toString());

        if (responseJson.has("RESULT")) {
          String resultBase64 = responseJson.getAsJsonObject("RESULT").getAsJsonPrimitive("base64result").getAsString();
          byte[] resultSerialized = Base64.decodeBase64(resultBase64);

          ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(resultSerialized);
          ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
          nsInfo = (NamespaceInfo) objectInputStream.readObject();
        } else if (responseJson.has("EXCEPTION")) {
          String exception = responseJson.getAsJsonPrimitive("EXCEPTION").getAsString();
          LOG.error("Exception encountered during Serverless NameNode execution.");
          LOG.error(exception);
        }

        //nsInfo = bpNamenode.versionRequest();
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch (SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch (IOException e) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }

      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }

    if (nsInfo != null) {
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nnVersion = nsInfo.getSoftwareVersion();
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive =
          new IncorrectVersionException(minimumNameNodeVersion, nnVersion,
              "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }
  }

  private void connectToNNAndHandshake() throws IOException {
    LOG.info("Performing handshake with NameNode now...");

    // get NN proxy
    // bpNamenode = dn.connectToNN(nnAddr);

    // First phase of the handshake with NN - get the namespace info.
    NamespaceInfo nsInfo = retrieveNamespaceInfo();

    // Verify that this matches the other NN in this HA pair.
    // This also initializes our block pool in the DN if we are
    // the first NN connection for this BP.
    bpos.verifyAndSetNamespaceInfo(nsInfo);

    // Second phase of the handshake with the NN.
    register(nsInfo);
  }

  void reportBadBlocks(ExtendedBlock block, String storageUuid,
      StorageType storageType) throws IOException {
    if (bpRegistration == null) {
      return;
    }
    DatanodeInfo[] dnArr = {new DatanodeInfo(bpRegistration)};
    String[] uuids = { storageUuid };
    StorageType[] types = { storageType };
    LocatedBlock[] blocks = { new LocatedBlock(block, dnArr, uuids, types) };

    try {
      bpNamenode.reportBadBlocks(blocks);
    } catch (IOException e) {
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block " + block + " to namenode : " +
          " Exception", e);
      //HOP we will retry by sending it to another nn. it could be because of NN failue
      throw e;
    }
  }

  @VisibleForTesting
  synchronized void triggerHeartbeatForTests() {
    final long nextHeartbeatTime = scheduler.scheduleHeartbeat();
    this.notifyAll();
    while (nextHeartbeatTime - scheduler.nextHeartbeatTime >= 0) {
      try {
        this.wait(100);
      } catch (InterruptedException e) {
        return;
      }
    }
  }
  
  HeartbeatResponse sendHeartBeat() throws IOException {
    StorageReport[] reports =
        dn.getFSDataset().getStorageReports(bpos.getBlockPoolId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat with " + reports.length +
          " storage reports from service actor: " + this);
    }
    
    VolumeFailureSummary volumeFailureSummary = dn.getFSDataset()
        .getVolumeFailureSummary();
    int numFailedVolumes = volumeFailureSummary != null ?
        volumeFailureSummary.getFailedStorageLocations().length : 0;
    return bpNamenode.sendHeartbeat(bpRegistration,
        reports,
        dn.getFSDataset().getCacheCapacity(),
        dn.getFSDataset().getCacheUsed(),
        dn.getXmitsInProgress(),
        dn.getXceiverCount(),
        numFailedVolumes,
        volumeFailureSummary);
  }

  //This must be called only by BPOfferService
  void start() {
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }

  private String formatThreadName() {
    Collection<StorageLocation> dataDirs = DataNode.getStorageLocations(
        dn.getConf());
    return "DataNode: [" + dataDirs.toString() + "] " +
        " heartbeating to " + nnAddr;
  }

  //This must be called only by blockPoolManager.
  void stop() {
    shouldServiceRun = false;
    if (bpThread != null) {
      bpThread.interrupt();
    }
  }

  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) {
    }
  }

  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {

    shouldServiceRun = false;
    IOUtils.cleanup(LOG, bpNamenode);

    bpos.shutdownActor(this);


  }

  private void handleRollingUpgradeStatus(HeartbeatResponse resp) throws IOException {
    RollingUpgradeStatus rollingUpgradeStatus = resp.getRollingUpdateStatus();
    if (rollingUpgradeStatus != null &&
        rollingUpgradeStatus.getBlockPoolId().compareTo(bpos.getBlockPoolId()) != 0) {
      // Can this ever occur?
      LOG.error("Invalid BlockPoolId " +
          rollingUpgradeStatus.getBlockPoolId() +
          " in HeartbeatResponse. Expected " +
          bpos.getBlockPoolId());
    } else {
      bpos.signalRollingUpgrade(rollingUpgradeStatus != null);
    }
  }
    
  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using"
        + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval + "msec"
        + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec"
        + " Initial delay: " + dnConf.initialBlockReportDelay + "msec"
        + "; heartBeatInterval=" + dnConf.heartBeatInterval);


    bpos.startWhirlingSufiThread();


    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        long startTime = scheduler.monotonicNow();

        //
        // Every so often, send heartbeat or block-report
        //
        final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
        if (sendHeartbeat) {

          refreshNNConnections();

          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          scheduler.scheduleNextHeartbeat();
          if (!dn.areHeartbeatsDisabledForTests()) {
            HeartbeatResponse resp = sendHeartBeat();
            assert resp != null;

            connectedToNN = true;

            dn.getMetrics().addHeartbeat(scheduler.monotonicNow() - startTime);

            handleRollingUpgradeStatus(resp);
            
            long startProcessCommands = monotonicNow();
            if (!processCommand(resp.getCommands())) {
              continue;
            }
            long endProcessCommands = monotonicNow();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands) +
                  "ms to process " + resp.getCommands().length +
                  " commands from NN");
            }
          }
        }

        long waitTime = scheduler.getHeartbeatWaitTime();
        if (waitTime <= 0) {
          // above code took longer than dnConf.heartBeatInterval to execute
          // set wait time to 1 ms to send a new HB immediately
          waitTime = 1;
        }
        Thread.sleep(waitTime);

      } catch (RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn(this + " is shutting down", re);
          shouldServiceRun = false;
          return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (InterruptedException e) {
        LOG.warn("OfferService interrupted", e);
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
        //not connected to namenode
        connectedToNN = false;
      }
    } // while (shouldRun())
  } // offerService

    /**
   * Register one bp with the corresponding NameNode
   * <p/>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and
   * 2) to receive a registrationID
   * <p/>
   * issued by the namenode to recognize registered datanodes.
   * 
   * @param nsInfo current NamespaceInfo
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   */
  void register(NamespaceInfo nsInfo) throws IOException {
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info
    bpRegistration = bpos.createRegistration();

    /*
    This is the old registration code. We are not registering with a serverful namenode. So we just skip this.

    while (shouldRun()) {
      try {
        // Use returned registration from namenode with updated fields
        bpRegistration = bpNamenode.registerDatanode(bpRegistration);
        bpRegistration.setNamespaceInfo(nsInfo);
        break;
      } catch(EOFException e) {  // namenode might have just restarted
        LOG.info("Problem connecting to server: " + nnAddr + " :"
            + e.getLocalizedMessage());
        sleepAndLogInterrupts(1000, "connecting to server");
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    */

    LOG.info("Block pool " + this + " successfully registered with NN");
    bpos.registrationSucceeded(this, bpRegistration);

    refreshNNConnections();

    // random short delay - helps scatter the BR from all DNs
    // block report only if the datanode is not already connected
    // to any other namenode.
    // In case of multiple actors (Multi NN)  we would like to 
    // send only one BR. 
    // potential race condition here. multiple actor might see that
    // none of the other actors are yet connected. to avoid such
    // scenarios we ask the first actor to do BR
    if(!bpos.otherActorsConnectedToNNs(this) && bpos.firstActor(this)) {
      bpos.scheduleBlockReport(dnConf.initialBlockReportDelay);
    } else {
      LOG.info("Block Report skipped as other BPServiceActors are connected to the namenodes ");
    }
  }


  private void sleepAndLogInterrupts(int millis, String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this + " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   * <p/>
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      while (true) {
        // init stuff
        try {
          // setup storage
          connectToNNAndHandshake();
          break;
        } catch (IOException ioe) {
          // Initial handshake, storage recovery or registration failed
          runningState = RunningState.INIT_FAILED;
          if (shouldRetryInit()) {
            // Retry until all namenode's of BPOS failed initialization
            LOG.error("Initialization failed for " + this + " "
                + ioe.getLocalizedMessage());
            sleepAndLogInterrupts(5000, "initializing");
          } else {
            runningState = RunningState.FAILED;
            LOG.fatal("Initialization failed for " + this + ". Exiting. ", ioe);
            cleanUp();
            return;
          }
        }
      }

      runningState = RunningState.RUNNING;

      while (shouldRun()) {
        try {
          offerService();
        } catch (Exception ex) {
          LOG.error("Exception in BPOfferService for " + this, ex);
          cleanUp(); //cean up and return. if the nn comes back online then it will be restarted
        }
      }
      runningState = RunningState.EXITED;
    } catch (Throwable ex) {
      LOG.warn("Unexpected exception in block pool " + this, ex);
      runningState = RunningState.FAILED;
    } finally {
      LOG.warn("Ending block pool service for: " + this);
      cleanUp(); //cean up and return. if the nn comes back online then it will be restarted
    }
  }

  private boolean shouldRetryInit() {
    return shouldRun() && bpos.shouldRetryInit();
  }
    
  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   *
   * @param cmds
   *     an array of datanode commands
   * @return true if further processing may be required or false otherwise.
   */
  boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (!bpos.processCommandFromActor(cmd, this)) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }

  void trySendErrorReport(int errCode, String errMsg) {
    try {
      bpNamenode.errorReport(bpRegistration, errCode, errMsg);
    } catch (IOException e) {
      LOG.warn("Error reporting an error to NameNode " + nnAddr, e);
    }
  }

  /**
   * Report a bad block from another DN in this cluster.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block)
      throws IOException {
    LocatedBlock lb = new LocatedBlock(block, new DatanodeInfo[]{dnInfo});
    bpNamenode.reportBadBlocks(new LocatedBlock[]{lb});
  }

  void reRegister() throws IOException {
    if (shouldRun()) {
      // re-retrieve namespace info to make sure that, if the NN
      // was restarted, we still match its version (HDFS-2120)
      NamespaceInfo nsInfo = retrieveNamespaceInfo();
      // and re-register
      register(nsInfo);
      scheduler.scheduleHeartbeat();
    }
  }


  @Override
  public boolean equals(Object obj) {
    if(obj == null || !(obj instanceof BPServiceActor)) {
      return false;
    }
    //Two actors are same if they are connected to save NN
    BPServiceActor that = (BPServiceActor) obj;
    return this.getNNSocketAddress().equals(that.getNNSocketAddress());
  }

  /**
   * get the list of active namenodes in the system. It connects to new
   * namenodes and stops the threads connected to dead namenodes
   */
  private void refreshNNConnections() throws IOException {
    if (!bpos.canUpdateNNList()) {
      return;
    }

    SortedActiveNodeList list = this.bpNamenode.getActiveNamenodes();
    bpos.updateNNList(list);
    bpos.setLastNNListUpdateTime();
  }

  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
    if (bpNamenode != null) {
      bpNamenode.blockReceivedAndDeleted(registration, poolId,
          receivedAndDeletedBlocks);
    }
  }

  public DatanodeCommand reportHashes(DatanodeRegistration registration,
                                     String poolId, StorageBlockReport[] reports) throws IOException {
    return bpNamenode.reportHashes(registration, poolId, reports);
  }

  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, StorageBlockReport[] reports, BlockReportContext context) throws IOException {
    return bpNamenode.blockReport(registration, poolId, reports, context);
  }

  public void blockReportCompleted(DatanodeRegistration registration, DatanodeStorage[] storages, boolean success)
      throws IOException {
    bpNamenode.blockReportCompleted(registration, storages, success);
  }

  public DatanodeCommand cacheReport(DatanodeRegistration bpRegistration,
                                     String bpid, List<Long> blockIds) throws IOException {
    return bpNamenode.cacheReport(bpRegistration, bpid, blockIds,
            dn.getFSDataset().getCacheCapacity(), dn.getFSDataset().getCacheUsed());
  }
  
  public ActiveNode nextNNForBlkReport(long noOfBlks,
                                       DatanodeRegistration nodeReg) throws IOException {
    if (bpNamenode != null) {
      return bpNamenode.getNextNamenodeToSendBlockReport(noOfBlks, nodeReg);
    } else {
      return null;
    }
  }

  public byte[] getSmallFileDataFromNN(int id)throws IOException {
    if (bpNamenode != null) {
      return bpNamenode.getSmallFileData(id);
    } else {
      return null;
    }
  }

  public boolean connectedToNN(){
    return connectedToNN;
  }
  
  /**
   * Utility class that wraps the timestamp computations for scheduling
   * heartbeats and block reports.
   */
  static class Scheduler {
    // nextHeartbeatTime may be assigned/read
    // by testing threads (through BPServiceActor#triggerXXX), while also
    // assigned/read by the actor thread.
    @VisibleForTesting
    volatile long nextHeartbeatTime = monotonicNow();

    private final long heartbeatIntervalMs;

    Scheduler(long heartbeatIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    // This is useful to make sure NN gets Heartbeat before Blockreport
    // upon NN restart while DN keeps retrying Otherwise,
    // 1. NN restarts.
    // 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
    // 3. After reregistration completes, DN will send Blockreport first.
    // 4. Given NN receives Blockreport after Heartbeat, it won't mark
    //    DatanodeStorageInfo#blockContentsStale to false until the next
    //    Blockreport.
    long scheduleHeartbeat() {
      nextHeartbeatTime = monotonicNow();
      return nextHeartbeatTime;
    }

    long scheduleNextHeartbeat() {
      // Numerical overflow is possible here and is okay.
      nextHeartbeatTime += heartbeatIntervalMs;
      return nextHeartbeatTime;
    }

    boolean isHeartbeatDue(long startTime) {
      return (nextHeartbeatTime - startTime <= 0);
    }

    long getHeartbeatWaitTime() {
      return nextHeartbeatTime - monotonicNow();
    }

    /**
     * Wrapped for testing.
     * @return
     */
    @VisibleForTesting
    public long monotonicNow() {
      return Time.monotonicNow();
    }
  }
}
