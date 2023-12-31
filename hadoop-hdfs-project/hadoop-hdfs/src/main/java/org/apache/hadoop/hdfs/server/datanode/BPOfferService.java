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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.mysql.clusterj.ClusterJDatastoreException;
import io.hops.exception.StorageInitializtionException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ExceptionCheck;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingNonLeaderException;
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingOverloadException;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * One instance per block-pool/namespace on the DN, which handles the
 * heartbeats
 * to the active and standby NNs for that namespace. This class manages an
 * instance of {@link BPServiceActor} for each NN, and delegates calls to both
 * NNs. It also maintains the state about which of the NNs is considered
 * active.
 */
@InterfaceAudience.Private
class BPOfferService implements Runnable {

  static final Log LOG = DataNode.LOG;

  /**
   * Information about the namespace that this service is registering with.
   * This
   * is assigned after the first phase of the handshake.
   */
  NamespaceInfo bpNSInfo;

  /**
   * The registration information for this block pool. This is assigned after
   * the second phase of the handshake.
   */
  volatile DatanodeRegistration bpRegistration;
  private final DataNode dn;
  /**
   * A reference to the BPServiceActor associated with the currently ACTIVE NN.
   * In the case that all NameNodes are in STANDBY mode, this can be null. If
   * non-null, this must always refer to a member of the {@link #bpServices}
   * list.
   */
  private BPServiceActor bpServiceToActive = null;
  /**
   * The list of all actors for namenodes in this nameservice, regardless of
   * their active or standby states.
   */
  private List<BPServiceActor> bpServices =
      new CopyOnWriteArrayList<>();
  /**
   * Each time we receive a heartbeat from a NN claiming to be ACTIVE, we
   * record
   * that NN's most recent transaction ID here, so long as it is more recent
   * than the previous value. This allows us to detect split-brain scenarios in
   * which a prior NN is still asserting its ACTIVE state but with a too-low
   * transaction ID. See HDFS-2627 for details.
   */
  private long lastActiveClaimTxId = -1;

  private final ReentrantReadWriteLock mReadWriteLock =
      new ReentrantReadWriteLock();
  private final Lock mReadLock  = mReadWriteLock.readLock();
  private final Lock mWriteLock = mReadWriteLock.writeLock();
  // utility methods to acquire and release read lock and write lock
  void readLock() {
    mReadLock.lock();
  }
  void readUnlock() {
    mReadLock.unlock();
  }
  void writeLock() {
    mWriteLock.lock();
  }
  void writeUnlock() {
    mWriteLock.unlock();
  }
  
  private final DNConf dnConf;
  // IBR = Incremental Block Report. If this flag is set then an IBR will be
  // sent immediately by the actor thread without waiting for the IBR timer
  // to elapse.
  private volatile boolean sendImmediateIBR = false;
    
  volatile long lastCacheReport = 0;
  private final Scheduler scheduler;
    
  private BPServiceActor blkReportHander = null;
  private List<ActiveNode> nnList = Collections.synchronizedList(new ArrayList<ActiveNode>());
  private List<InetSocketAddress> blackListNN = Collections.synchronizedList(new ArrayList<InetSocketAddress>());
  // private Object nnListSync = new Object();
  private AtomicInteger rpcRoundRobinIndex = new AtomicInteger(0);
  // you have bunch of NNs, which one to send the incremental block report
  private AtomicInteger refreshNNRoundRobinIndex = new AtomicInteger(0);
  //in a heart beat only one actor should talk to name node and get the updated list of NNs
  //how to stop actors from communicating with all the NN at the same time for same RPC?
  //for that we will use a separate RR which will be incremented after Delta time (heartbeat time)
  /**
   * Between block reports (which happen on the order of once an hour) the DN
   * reports smaller incremental changes to its block list. This map, keyed by
   * block ID, contains the pending changes which have yet to be reported to
   * the
   * NN. Access should be synchronized on this object.
   */
  private final Map<DatanodeStorage, PerStoragePendingIncrementalBR>
      pendingIncrementalBRperStorage = Maps.newHashMap();

  private Thread blockReportThread = null;

  private Random rand = new Random(System.currentTimeMillis());
  private long prevBlockReportId;

  BPOfferService(List<InetSocketAddress> nnAddrs, DataNode dn) throws StorageInitializtionException {
    Preconditions
        .checkArgument(!nnAddrs.isEmpty(), "Must pass at least one NN.");
    this.dn = dn;

    for (InetSocketAddress addr : nnAddrs) {
      this.bpServices.add(new BPServiceActor(addr, this));
      nnList.add(new ActiveNodePBImpl(0, "", addr.getAddress().getHostAddress(),
          addr.getPort(), "", addr.getAddress().getHostAddress(), addr.getPort()));
    }

    dnConf = dn.getDnConf();
    prevBlockReportId = DFSUtil.getRandom().nextLong();
    scheduler = new Scheduler(dnConf.heartBeatInterval, dnConf.blockReportInterval);
  }

  void refreshNNList(ArrayList<InetSocketAddress> addrs) throws IOException {
    Set<InetSocketAddress> oldAddrs = Sets.newHashSet();
    for (BPServiceActor actor : bpServices) {
      oldAddrs.add(actor.getNNSocketAddress());
    }
    Set<InetSocketAddress> newAddrs = Sets.newHashSet(addrs);


    SetView<InetSocketAddress> deadNNs = Sets.difference(oldAddrs, newAddrs);
    SetView<InetSocketAddress> newNNs = Sets.difference(newAddrs, oldAddrs);

    // stop the dead threads
    if (deadNNs.size() != 0) {
      for (InetSocketAddress deadNN : deadNNs) {
        BPServiceActor deadActor = stopAnActor(deadNN);
        bpServices.remove(
            deadActor); // NNs will not change frequently. so modification ops will not be expensive on the copyonwirte list
        LOG.debug("Stopped actor for " + deadActor.getNNSocketAddress());
      }
    }

    // start threads for new NNs
    if (newNNs.size() != 0) {
      for (InetSocketAddress newNN : newNNs) {
        BPServiceActor newActor = startAnActor(newNN);
        bpServices.add(
            newActor); // NNs will not change frequently. so modification ops will not be expensive on the copyonwirte list
        LOG.debug("Started actor for " + newActor.getNNSocketAddress());
      }
    }

  }

  /**
   * @return true if the service has registered with at least one NameNode.
   */
  boolean isInitialized() {
    return bpRegistration != null;
  }

  /**
   * @return true if there is at least one actor thread running which is talking
   * to a NameNode.
   */
  boolean isAlive() {
    for (BPServiceActor actor : bpServices) {
      if (actor.isAlive()) {
        return true;
      }
    }
    return false;
  }

  String getBlockPoolId() {
    readLock();
    try {
      if (bpNSInfo != null) {
        return bpNSInfo.getBlockPoolID();
      } else {
        LOG.warn("Block pool ID needed, but service not yet registered with NN",
            new Exception("trace"));
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  boolean hasBlockPoolId() {
    return getNamespaceInfo() != null;
  }
    
    NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return bpNSInfo;
    } finally {
      readUnlock();
    }
  }

  @Override
  public String toString() {
    readLock();
    try {
      if (bpNSInfo == null) {
        // If we haven't yet connected to our NN, we don't yet know our
        // own block pool ID.
        // If _none_ of the block pools have connected yet, we don't even
        // know the DatanodeID ID of this DN.
        String datanodeUuid = dn.getDatanodeUuid();
        if (datanodeUuid == null || datanodeUuid.isEmpty()) {
          datanodeUuid = "unassigned";
        }
        return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
      } else {
        return "Block pool " + getBlockPoolId() +
            " (Datanode Uuid " + dn.getDatanodeUuid() +
            ")";
      }
    } finally {
      readUnlock();
    }
  }

  void reportBadBlocks(ExtendedBlock block, String storageUuid, StorageType storageType) {
    checkBlock(block);
    try {
      reportBadBlocksWithRetry(block, storageUuid, storageType);
    } catch (Exception e) {
      LOG.error("Failed to send bad block report to any namenode ", e);
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint,
      String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECEIVED_BLOCK, delHint);

    notifyNamenodeBlockInt(bInfo, storageUuid, true);
  }

  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null, "block is null");
    Preconditions.checkArgument(block.getBlockPoolId().equals(getBlockPoolId()),
        "block belongs to BP %s instead of BP %s", block.getBlockPoolId(),
        getBlockPoolId());
  }

  void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.DELETED_BLOCK, null);

    notifyNamenodeDeletedBlockInt(bInfo, dn.getFSDataset().getStorage(storageUuid));
  }
  
  public void notifyNamenodeReceivingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECEIVING_BLOCK, null);
    notifyNamenodeBlockInt(bInfo, storageUuid, false);
  }

  public void notifyNamenodeAppendingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.APPENDING, null);
    notifyNamenodeBlockInt(bInfo, storageUuid, false);
  }

  public void notifyNamenodeAppendingRecoveredAppend(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECOVERING_APPEND, null);
    notifyNamenodeBlockInt(bInfo, storageUuid, true);
  }

  public void notifyNamenodeUpdateRecoveredBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.UPDATE_RECOVERED, null);
    notifyNamenodeBlockInt(bInfo, storageUuid, true);
  }


  //This must be called only by blockPoolManager
  void start() {
    for (BPServiceActor actor : bpServices) {
      actor.start();
    }
  }

  //This must be called only by blockPoolManager.
  void stop() {
    for (BPServiceActor actor : bpServices) {
      actor.stop();
    }
  }

  //This must be called only by blockPoolManager
  void join() {
    for (BPServiceActor actor : bpServices) {
      actor.join();
    }
  }

  DataNode getDataNode() {
    return dn;
  }

  /**
   * Called by the BPServiceActors when they handshake to a NN. If this is the
   * first NN connection, this sets the namespace info for this BPOfferService.
   * If it's a connection to a new NN, it verifies that this namespace matches
   * (eg to prevent a misconfiguration where a StandbyNode from a different
   * cluster is specified)
   */
  void verifyAndSetNamespaceInfo(NamespaceInfo nsInfo) throws IOException {
    writeLock();
    try {
      if (this.bpNSInfo == null) {
        this.bpNSInfo = nsInfo;
        boolean success = false;
        // Now that we know the namespace ID, etc, we can pass this to the DN.
        // The DN can now initialize its local storage if we are the
        // first BP to handshake, etc.
        try {
          dn.initBlockPool(this);
          long groupId = nsInfo.getGroupId();
          LOG.debug("Assigning groupId of " + groupId + " to this DataNode.");
          dn.setStorageReportGroupCounter(groupId);
          success = true;
        } finally {
          if (!success) {
            // The datanode failed to initialize the BP. We need to reset
            // the namespace info so that other BPService actors still have
            // a chance to set it, and re-initialize the datanode.
            this.bpNSInfo = null;
          }
        }
      } else {
        checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
            "Blockpool ID");
        checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
            "Namespace ID");
        checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
            "Cluster ID");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * After one of the BPServiceActors registers successfully with the NN, it
   * calls this function to verify that the NN it connected to is consistent
   * with other NNs serving the block-pool.
   */
  void registrationSucceeded(BPServiceActor bpServiceActor,
      DatanodeRegistration reg) throws IOException {
    writeLock();
    try {
      if (bpRegistration != null) {
        checkNSEquality(bpRegistration.getStorageInfo().getNamespaceID(),
            reg.getStorageInfo().getNamespaceID(), "namespace ID");
        checkNSEquality(bpRegistration.getStorageInfo().getClusterID(),
            reg.getStorageInfo().getClusterID(), "cluster ID");
      } else {
        bpRegistration = reg;
      }
      dn.bpRegistrationSucceeded(bpRegistration, getBlockPoolId());
      // Add the initial block token secret keys to the DN's secret manager.
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(getBlockPoolId(),
            reg.getExportedKeys());
      }

      // Now that the DataNode has its ID field populated (this occurs in the bpRegistrationSucceeded() function),
      // we can have the DataNode write its MetaData to intermediate storage.
      dn.writeMetadataToIntermediateStorage();

    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify equality of two namespace-related fields, throwing an exception if
   * they are unequal.
   */
  private static void checkNSEquality(Object ourID, Object theirID,
      String idHelpText) throws IOException {
    if (!ourID.equals(theirID)) {
      throw new IOException(
          idHelpText + " mismatch: " + "previously connected to " + idHelpText +
              " " + ourID + " but now connected to " + idHelpText + " " +
              theirID);
    }
  }

  DatanodeRegistration createRegistration() {
    writeLock();
    try {
      Preconditions.checkState(bpNSInfo != null,
          "getRegistration() can only be called after initial handshake");
      return dn.createBPRegistration(bpNSInfo);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Called when an actor shuts down. If this is the last actor to shut down,
   * shuts down the whole blockpool in the DN.
   */
  void shutdownActor(BPServiceActor actor) {
    writeLock();
    try {
      if (bpServiceToActive == actor) {
        bpServiceToActive = null;
      }

      bpServices.remove(actor);

      // remove from nnList
      for (ActiveNode ann : nnList) {
        if (ann.getRpcServerAddressForDatanodes().equals(actor.getNNSocketAddress())) {
          nnList.remove(ann);
          break;
        }
      }

      if (bpServices.isEmpty()) {
        dn.shutdownBlockPool(this);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Called by the DN to report an error to the NNs.
   */
  void trySendErrorReport(int errCode, String errMsg) {
    //HOP error report should be sent to all the NN.
    //Leader will delete the blocks and clear the in meomory data structs from Datanode manager and HB Manager.
    //Non leader NNs will only clear the in memory data structures.
    for (BPServiceActor actor : bpServices) {
      actor.trySendErrorReport(errCode, errMsg);
    }
  }

  /**
   * Ask each of the actors to schedule a block report after the specified
   * delay.
   */
  void scheduleBlockReport(long delay) {
    scheduler.scheduleBlockReport(delay);
  }

  public boolean otherActorsConnectedToNNs(BPServiceActor skip){
    try{
      readLock();
      for (BPServiceActor actor : bpServices) {
        if(actor != skip){
          if (actor.connectedToNN()) {
            return true;
          }
        }
      }
      return false;
    }finally {
      readUnlock();
    }
  }

  /**
   * Ask each of the actors to report a bad block hosted on another DN.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block) {
    try {
      reportRemoteBadBlockWithRetry(dnInfo, block);
    } catch (IOException e) {
      LOG.warn("Couldn't report bad block " + block + "" + e);
    }
  }

  /**
   * @return a proxy to the active NN, or null if the BPOS has not acknowledged
   * any NN as active yet.
   */
   DatanodeProtocolClientSideTranslatorPB getActiveNN() {
    readLock();
    try {
      if (bpServiceToActive != null) {
        return bpServiceToActive.bpNamenode;
      } else {
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  List<BPServiceActor> getBPServiceActors() {
    return Lists.newArrayList(bpServices);
  }

  /**
   * Signal the current rolling upgrade status as indicated by the NN.
   * @param inProgress true if a rolling upgrade is in progress
   */
  void signalRollingUpgrade(boolean inProgress) throws IOException {
    String bpid = getBlockPoolId();
    if (inProgress) {
      dn.getFSDataset().enableTrash(bpid);
      dn.getFSDataset().setRollingUpgradeMarker(bpid);
    } else {
      dn.getFSDataset().restoreTrash(bpid);
      dn.getFSDataset().clearRollingUpgradeMarker(bpid);
    }
  }
  
  /**
   * @return true if the given NN address is one of the NNs for this block pool
   */
  boolean containsNN(InetSocketAddress addr) {
    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(addr)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  int countNameNodes() {
    return bpServices.size();
  }

  /**
   * Run an immediate deletion report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerDeletionReportForTests() throws IOException {
    triggerDeletionReportForTestsInt();
  }

  /**
   * Run an immediate heartbeat from all actors. Used by tests.
   */
  @VisibleForTesting
  void triggerHeartbeatForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerHeartbeatForTests();
    }
  }

  boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {

    assert bpServices.contains(actor);
    
    if (cmd == null) {
      return true;
    }

    /*
     * Datanode Registration can be done asynchronously here. No need to hold
     * the lock. for more info refer HDFS-5014
     */
    if (DatanodeProtocol.DNA_REGISTER == cmd.getAction()) {
      LOG.info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr);
      actor.reRegister();
      return true;
    }
    
    writeLock();
    try{
      return processCommandFromActive(cmd, actor);
    } finally {
      writeUnlock();
    }
  }

  private String blockIdArrayToString(long ids[]) {
    long maxNumberOfBlocksToLog = dn.getMaxNumberOfBlocksToLog();
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (int i = 0; i < ids.length; i++) {
      if (i >= maxNumberOfBlocksToLog) {
        bld.append("...");
        break;
      }
      bld.append(prefix).append(ids[i]);
      prefix = ", ";
    }
    return bld.toString();
  }
  
  /**
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    final BlockCommand bcmd =
        cmd instanceof BlockCommand ? (BlockCommand) cmd : null;
    final BlockIdCommand blockIdCmd = 
      cmd instanceof BlockIdCommand ? (BlockIdCommand)cmd: null;
    
    switch (cmd.getAction()) {
      case DatanodeProtocol.DNA_TRANSFER:
        // Send a copy of a block to another datanode
        dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
            bcmd.getTargets(), bcmd.getTargetStorageTypes());
        break;
      case DatanodeProtocol.DNA_INVALIDATE:
        //
        // Some local block(s) are obsolete and can be
        // safely garbage-collected.
        //
        Block toDelete[] = bcmd.getBlocks();
        try {
          // using global fsdataset
          dn.getFSDataset().invalidate(bcmd.getBlockPoolId(), toDelete);
        } catch (IOException e) {
          // Exceptions caught here are not expected to be disk-related.
          throw e;
        }
        dn.metrics.incrBlocksRemoved(toDelete.length);
        break;
      case DatanodeProtocol.DNA_CACHE:
        LOG.info("DatanodeCommand action: DNA_CACHE for " + blockIdCmd.getBlockPoolId() + " of ["
            + blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
        dn.getFSDataset().cache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
        break;
      case DatanodeProtocol.DNA_UNCACHE:
        LOG.info("DatanodeCommand action: DNA_UNCACHE for " + blockIdCmd.getBlockPoolId() + " of ["
            + blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
        dn.getFSDataset().uncache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
        break;
      case DatanodeProtocol.DNA_SHUTDOWN:
        // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
        // See HDFS-2987.
        throw new UnsupportedOperationException(
            "Received unimplemented DNA_SHUTDOWN");
      case DatanodeProtocol.DNA_FINALIZE:
        String bp = ((FinalizeCommand) cmd).getBlockPoolId();
        assert getBlockPoolId().equals(bp) :
            "BP " + getBlockPoolId() + " received DNA_FINALIZE " +
                "for other block pool " + bp;

        dn.finalizeUpgradeForPool(bp);
        break;
      case DatanodeProtocol.DNA_RECOVERBLOCK:
        String who = "NameNode at " + actor.getNNSocketAddress();
        dn.recoverBlocks(who,
            ((BlockRecoveryCommand) cmd).getRecoveringBlocks());
        break;
      case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
        LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
        if (dn.isBlockTokenEnabled) {
          dn.blockPoolTokenSecretManager.addKeys(getBlockPoolId(),
              ((KeyUpdateCommand) cmd).getExportedKeys());
        }
        break;
      case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
        LOG.info("DatanodeCommand action: DNA_BALANCERBANDWIDTHUPDATE");
        long bandwidth =
            ((BalancerBandwidthCommand) cmd).getBalancerBandwidthValue();
        if (bandwidth > 0) {
          DataXceiverServer dxcs =
              (DataXceiverServer) dn.dataXceiverServer.getRunnable();
          LOG.info("Updating balance throttler bandwidth from " +
              dxcs.balanceThrottler.getBandwidth() + " bytes/s " + "to: " +
              bandwidth + " bytes/s.");
          dxcs.balanceThrottler.setBandwidth(bandwidth);
        }
        break;
      default:
        LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }


  private BPServiceActor stopAnActor(InetSocketAddress address) {

    BPServiceActor actor = getAnActor(address);
    if (actor != null) {
      actor.stop();
//      actor.join();
      return actor;
    } else {
      return null;
    }
  }

  private BPServiceActor startAnActor(InetSocketAddress address) throws StorageInitializtionException {
    BPServiceActor actor = new BPServiceActor(address, this);
    actor.start();
    return actor;
  }

  private BPServiceActor getAnActor(InetSocketAddress address) {
    if (address == null) {
      return null;
    }

    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(address)) {
        return actor;
      }
    }
    return null;
  }

  /**
   * Main loop for each BP thread. Run until shutdown, forever calling remote NameNode functions.
   */
  private void whirlingLikeASufi()
      throws Exception {   //http://en.wikipedia.org/wiki/Sufi_whirling

    while (dn.shouldRun) {  //as long as datanode is alive keep working
      try {
        long startTime = scheduler.monotonicNow();
        boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);

        if (sendHeartbeat) {
            scheduler.scheduleNextHeartbeat();
        }
        
        if (sendImmediateIBR || sendHeartbeat) {
          reportReceivedDeletedBlocks();
        }

        List<DatanodeCommand> cmds = blockReport();
        if (cmds != null && blkReportHander != null) { //it is not null if the block report is successful
          blkReportHander.processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));
        }

        DatanodeCommand cmd = cacheReport(cmds != null);
        if (cmd != null && blkReportHander != null) {
          blkReportHander.processCommand(new DatanodeCommand[]{cmd});
        }

        //
        // There is no work to do;  sleep until hearbeat timer elapses,
        // or work arrives, and then iterate again.
        //
        long waitTime = scheduler.getHeartbeatWaitTime();
        synchronized (pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {
            try {
              pendingIncrementalBRperStorage.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized

        forwardRRIndex();//after every 1000ms increment the refreshNNRoundRobinIndex
      } catch (Exception re) {
        LOG.warn("Exception in whirlingLikeASufi", re);
        try {
          long sleepTime = 1000;
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    } // while (shouldRun())
  } // offerService

  /**
   * Report received blocks and delete hints to the Namenode
   *
   * @throws IOException
   */
  public void reportReceivedDeletedBlocks() throws IOException {
    // Generate a list of the pending reports for each storage under the lock
    List<StorageReceivedDeletedBlocks> reports =
            new ArrayList<>(pendingIncrementalBRperStorage.size());
    synchronized (pendingIncrementalBRperStorage) {
      for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
              pendingIncrementalBRperStorage.entrySet()) {
        final DatanodeStorage storage = entry.getKey();
        final PerStoragePendingIncrementalBR perStorageMap = entry.getValue();
        if (perStorageMap.getBlockInfoCount() > 0) {
          // Send newly-received and deleted blockids to namenode
          ReceivedDeletedBlockInfo[] rdbi = perStorageMap.dequeueBlockInfos();
          reports.add(new StorageReceivedDeletedBlocks(storage, rdbi));
        }
      }
      sendImmediateIBR = false;
    }

    LOG.debug("DataNode has " + reports.size() + " incremental block report(s) to send to the NameNode.");

    if (reports.size() == 0) {
      // Nothing new to report.
      return;
    }

    // Send incremental block reports to the Namenode outside the lock
    boolean success = false;
    final long startTime = monotonicNow();
    try {
      blockReceivedAndDeletedWithRetry(reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]));
      success = true;
    } catch (ClusterJDatastoreException ex) {
      LOG.error("Encountered ClusterJDatastoreException exception while storing intermediate block reports in " +
              "intermediate storage...", ex);
      ex.printStackTrace();
      throw new IOException("Failed to store intermediate block reports in intermediate storage...");
    }
    finally {
      dn.getMetrics().addIncrementalBlockReport(monotonicNow() - startTime);
      if (!success) {
        synchronized (pendingIncrementalBRperStorage) {
          for (StorageReceivedDeletedBlocks report : reports) {
            // If we didn't succeed in sending the report, put all the blocks back onto our queue, but only
            // in the case where we didn't put something newer in the meantime.
            PerStoragePendingIncrementalBR perStorageMap = pendingIncrementalBRperStorage.get(report.getStorage());
            perStorageMap.putMissingBlockInfos(report.getBlocks());
            sendImmediateIBR = true;
          }
        }
      }
    }
    return;
  }

  /**
   * Retrieve the incremental BR state for a given storage UUID
   * @param storage
   * @return
   */
  private PerStoragePendingIncrementalBR getIncrementalBRMapForStorage(
      DatanodeStorage storage) {
    PerStoragePendingIncrementalBR mapForStorage =
        pendingIncrementalBRperStorage.get(storage);
    if (mapForStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      mapForStorage = new PerStoragePendingIncrementalBR();
      pendingIncrementalBRperStorage.put(storage, mapForStorage);
    }
    return mapForStorage;
  }
  /**
   * Add a blockInfo for notification to NameNode. If another entry
   * exists for the same block it is removed.
   *
   * Caller must synchronize access using pendingIncrementalBRperStorage.
   * @param bInfo
   * @param storage
   */
  boolean addPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo,
      DatanodeStorage storage) {
    // Make sure another entry for the same block is first removed.
    // There may only be one such entry.
    boolean isNew = true;
    for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
        pendingIncrementalBRperStorage.entrySet()) {
      if (entry.getValue().removeBlockInfo(bInfo)) {
        isNew = false;
        break;
      }
    }
    getIncrementalBRMapForStorage(storage).putBlockInfo(bInfo);
    return isNew;
  }

  /*
   * Informing the name node could take a long, long time! Should we wait
   * till NameNode is informed before responding with success to the
   * client? For now, we don't.
   */
  void notifyNamenodeBlockInt(ReceivedDeletedBlockInfo bInfo,
      String storageUuid, boolean now) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(bInfo, dn.getFSDataset().getStorage(storageUuid));
      sendImmediateIBR = true;
      // If now is true, the report is sent right away.
      // Otherwise, it will be sent out in the next heartbeat.
      if (now) {
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }

  void notifyNamenodeDeletedBlockInt(
      ReceivedDeletedBlockInfo bInfo, DatanodeStorage storage) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(bInfo, storage);
    }
  }

  /**
   * Report the list blocks to the NameNode.
   *
   * @throws IOException
   */
  
  List<DatanodeCommand> blockReport() throws IOException {
    // send block report if timer has expired.
    if (!scheduler.isBlockReportDue()) {
      return null;
    }

    scheduler.setNextBlockReportOverwritten(false);
    
    ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // Flush any block information that precedes the block report. Otherwise
    // we have a chance that we will miss the delHint information
    // or we will report an RBW replica after the BlockReport already reports
    // a FINALIZED one.
    reportReceivedDeletedBlocks();

    long brCreateStartTime = monotonicNow();
    Map<DatanodeStorage, BlockReport> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(getBlockPoolId());

    // Convert the reports to the format expected by the NN.
    int i = 0;
    int totalBlockCount = 0;
    StorageBlockReport reports[] =
        new StorageBlockReport[perVolumeBlockLists.size()];
    DatanodeStorage[] storages = new DatanodeStorage[reports.length];

    for(Map.Entry<DatanodeStorage, BlockReport> kvPair : perVolumeBlockLists.entrySet()) {
      BlockReport blockList = kvPair.getValue();
      LOG.debug("blockList.getBuckets()[0].getBlocks().getClass().getSimpleName() = " + blockList.getBuckets()[0].getBlocks().getClass().getSimpleName());
      reports[i] = new StorageBlockReport(kvPair.getKey(), blockList);
      totalBlockCount += blockList.getNumberOfBlocks();
      storages[i] = kvPair.getKey();
      i++;
    }

    // Get a NameNode to send the report(s) to
    ActiveNode an = nextNNForBlkReport(totalBlockCount);
    int numReportsSent = 0;
    int numRPCs = 0;
    boolean success = false;
    long brSendStartTime;
    try {
      if (an != null) {
        blkReportHander = getAnActor(an.getRpcServerAddressForDatanodes());
        if (blkReportHander == null || !blkReportHander.isRunning()) {
          return null; //no one is ready to handle the request, return now without changing the values of lastBlockReport. it will be retried in next cycle
        }
      } else {
        LOG.warn("Unable to send block report");
        return null;
      }

      // Send the reports to the NN.
      brSendStartTime = monotonicNow();
      long reportId = generateUniqueBlockReportId();
      try {
        if (totalBlockCount < dnConf.blockReportSplitThreshold) {
          // Below split threshold, send all reports in a single message.
          DatanodeCommand buckets = blkReportHander.reportHashes(bpRegistration, getBlockPoolId()
                  , slimBlockReports(reports));
          removeMatchingBuckets(buckets, reports);
          DatanodeCommand cmd = blkReportHander.blockReport(
              bpRegistration, getBlockPoolId(), reports,
              new BlockReportContext(1, 0, reportId));
          numRPCs = 1;
          numReportsSent = reports.length;
          if (cmd != null) {
            cmds.add(cmd);
          }
        } else {
          // Send one block report per message.
          for (int r = 0; r < reports.length; r++) {
            StorageBlockReport singleReport[] = { reports[r] };
            DatanodeCommand buckets = blkReportHander.reportHashes(bpRegistration,
                    getBlockPoolId(), slimBlockReports(singleReport));
            removeMatchingBuckets(buckets, singleReport);
            DatanodeCommand cmd = blkReportHander.blockReport(bpRegistration, getBlockPoolId(), singleReport,
                new BlockReportContext(reports.length, r, reportId));
            numReportsSent++;
            numRPCs++;
            if (cmd != null) {
              cmds.add(cmd);
            }
          }
        }
        success = true;
      } finally {
        // Log the block report processing stats from Datanode perspective
        long brSendCost = monotonicNow() - brSendStartTime;
        long brCreateCost = brSendStartTime - brCreateStartTime;
        dn.getMetrics().addBlockReport(brSendCost);
        dn.getMetrics().incrBlocReportCounter(numReportsSent);
        final int nCmds = cmds.size();
        LOG.info((success ? "S" : "Uns") +
          "uccessfully sent block report 0x" +
          Long.toHexString(reportId) + ",  containing " + reports.length +
          " storage report(s), of which we sent " + numReportsSent + "." +
          " The reports had " + totalBlockCount +
          " total blocks and used " + numRPCs +
          " RPC(s). This took " + brCreateCost +
          " msec to generate and " + brSendCost +
          " msecs for RPC and NN processing." +
          " Got back " +
          ((nCmds == 0) ? "no commands" :
              ((nCmds == 1) ? "one command: " + cmds.get(0) :
                  (nCmds + " commands: " + Joiner.on("; ").join(cmds)))) +
          ".");
      }
    } finally {
      // In case of un/successful block reports we have to inform the leader that
      // block reporting has finished for now.
      if (blkReportHander != null) {
        for (BPServiceActor actor : bpServices) {
          actor.blockReportCompleted(bpRegistration, storages, success);
        }
      }
    }

    scheduler.scheduleNextBlockReport();
    return cmds.size() == 0 ? null : cmds;
  }

  StorageBlockReport[] slimBlockReports(StorageBlockReport[] reports){
    StorageBlockReport[] slimStorageReports = new StorageBlockReport[reports.length];

    for(int i = 0 ; i < reports.length; i++){
      StorageBlockReport fatSR = reports[i];
      BlockReport fatBR = fatSR.getReport();
      Bucket slimBuckets[] = new Bucket[fatBR.getBuckets().length];

      for(int j = 0; j < fatBR.getBuckets().length; j++){
        Bucket slimBucket = new Bucket();
        slimBucket.setHash(fatBR.getBuckets()[j].getHash());
        slimBucket.setBlocks(BlockListAsLongs.EMPTY);
        slimBuckets[j] = slimBucket;
      }

      BlockReport slimBR = new BlockReport(slimBuckets, fatBR.getNumberOfBlocks());

      slimStorageReports[i] = new StorageBlockReport(fatSR.getStorage(), slimBR);
    }

    return slimStorageReports;
  }

  public void removeMatchingBuckets(DatanodeCommand cmd, StorageBlockReport reports[]){
    Map<String, List<Integer>> map = ((HashesMismatchCommand) cmd).getMissMatchingBuckets();
    for(StorageBlockReport report : reports){
      List<Integer> mismatchingBuckets = map.get(report.getStorage().getStorageID());
      removeMatchingBuckets(mismatchingBuckets,report.getReport());
    }
  }

  public static void removeMatchingBuckets(List<Integer> mismatchingBuckets, BlockReport report){
    HashSet mismatchingBucketsSet = new HashSet(mismatchingBuckets);
    for(int i = 0; i < report.getBuckets().length; i++){
      if(!mismatchingBucketsSet.contains(i)){
        report.getBuckets()[i].setBlocks(BlockListAsLongs.EMPTY);
        report.getBuckets()[i].setSkip(true);
      }
    }
  }

  DatanodeCommand cacheReport(boolean hasHandler) throws IOException {
    // If caching is disabled, do not send a cache report
    if (dn.getFSDataset().getCacheCapacity() == 0) {
      return null;
    }
    // send cache report if timer has expired.
    DatanodeCommand cmd = null;
    long startTime = monotonicNow();
    if (startTime - lastCacheReport > dnConf.cacheReportInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending cacheReport from service actor: " + this);
      }
      lastCacheReport = startTime;

      String bpid = getBlockPoolId();
      List<Long> blockIds = dn.getFSDataset().getCacheReport(bpid);
      long createTime = monotonicNow();

      if (!hasHandler) {
        if(!nnList.isEmpty()){
          ActiveNode an = nnList.get(rand.nextInt(nnList.size()));
          blkReportHander = getAnActor(an.getRpcServerAddressForDatanodes());
          if (blkReportHander == null || !blkReportHander.isRunning()) {
            return null; //no one is ready to handle the request, return now without changing the values of lastBlockReport. it will be retried in next cycle
          }
        }else {
          LOG.warn("Unable to send cache report");
          return null;
        }
      }
      
      cmd = blkReportHander.cacheReport(bpRegistration, bpid, blockIds);
      long sendTime = monotonicNow();
      long createCost = createTime - startTime;
      long sendCost = sendTime - createTime;
      dn.getMetrics().addCacheReport(sendCost);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CacheReport of " + blockIds.size()
            + " block(s) took " + createCost + " msec to generate and "
            + sendCost + " msecs for RPC and NN processing");
      }
    }
    return cmd;
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      long nextBlockReportTime = scheduler.scheduleBlockReport(0);
      pendingIncrementalBRperStorage.notifyAll();
      while (nextBlockReportTime - scheduler.nextBlockReportTime >= 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  void triggerDeletionReportForTestsInt() {
    synchronized (pendingIncrementalBRperStorage) {
      sendImmediateIBR = true;
      pendingIncrementalBRperStorage.notifyAll();

      while (sendImmediateIBR) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  boolean hasPendingIBR() {
    return sendImmediateIBR;
  }

  private long generateUniqueBlockReportId() {
    // Initialize the block report ID the first time through.
    // Note that 0 is used on the NN to indicate "uninitialized", so we should
    // not send a 0 value ourselves.
    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = DFSUtil.getRandom().nextLong();  
    }
    return prevBlockReportId;
  }
  
//  void updateNNList(SortedActiveNodeList list) throws IOException {
//    writeLock();
//    try {
//      ArrayList<InetSocketAddress> nnAddresses = new ArrayList<InetSocketAddress>();
//      for (ActiveNode ann : list.getActiveNodes()) {
//        nnAddresses.add(ann.getRpcServerAddressForDatanodes());
//      }
//
//      refreshNNList(nnAddresses);
//
//      if (list.getLeader() != null) {
//        bpServiceToActive = getAnActor(list.getLeader().getRpcServerAddressForDatanodes());
//      }
//
//      nnList.clear();
//      nnList.addAll(list.getActiveNodes());
//      blackListNN.clear();
//    } finally {
//      writeUnlock();
//    }
//  }

  long nnListLastUpdate = 0;

  boolean canUpdateNNList() {
    writeLock();
    try {
      if (nnList == null || nnList.size() == 0) {
        return true; // for edge case, any one can update. after that actors will take trun in updating the nnlist
      }

      if ((System.currentTimeMillis() - nnListLastUpdate) > 5000) {
        return true;
      } else {
        return false;
      }
    } finally {
      writeUnlock();
    }
  }

  void setLastNNListUpdateTime(){
    nnListLastUpdate = System.currentTimeMillis();
  }

  public synchronized void startWhirlingSufiThread() {
    if (blockReportThread == null || !blockReportThread.isAlive()) {
      LOG.debug("It's time to start whirling like a sufi!");
      blockReportThread = new Thread(this, "BlkReportThread");
      blockReportThread.setDaemon(true); // needed for JUnit testing
      blockReportThread.start();
    }
  }

  @Override
  public void run() {
    try {
      whirlingLikeASufi();
    } catch (Exception ex) {
      LOG.warn("Unexpected exception in BPOfferService " + this, ex);
    }
  }

  private void reportBadBlocksWithRetry(final ExtendedBlock block,
      final String storageUuid, final StorageType storageType)
      throws IOException {
    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.reportBadBlocks(block, storageUuid, storageType);
        return null;
      }
    });
  }

  private void blockReceivedAndDeletedWithRetry(
      final StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {

    String blocks = "";
    for(StorageReceivedDeletedBlocks srdb : receivedAndDeletedBlocks) {
      blocks += "[";
      for(ReceivedDeletedBlockInfo b : srdb.getBlocks()) {
        blocks += " " + b.getBlock().getBlockId() + b.toString();
      }
      blocks += "]";
    }
    LOG.info("sending blockReceivedAndDeletedWithRetry for blocks "
        + blocks);

    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.blockReceivedAndDeleted(bpRegistration, getBlockPoolId(),
            receivedAndDeletedBlocks);
        return null;
      }
    });
  }

  private void reportRemoteBadBlockWithRetry(final DatanodeInfo dnInfo,
      final ExtendedBlock block) throws IOException {
    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.reportRemoteBadBlock(dnInfo, block);
        return null;
      }
    });
  }

  public byte[] getSmallFileDataFromNN(final int id) throws IOException {
    byte[] data = (byte[]) doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        return actor.getSmallFileDataFromNN(id);
      }
    });
    return data;
  }

  public boolean firstActor(BPServiceActor actor) {
    if (bpServices.size()>0){
      if(bpServices.get(0).equals(actor)){
        return true;
      }
    }
    return  false;
  }

  private interface ActorActionHandler {

    Object doAction(BPServiceActor actor) throws IOException;
  }

  private Object doActorActionWithRetry(ActorActionHandler handler)
      throws IOException {
    Exception exception = null;
    boolean success = false;
    BPServiceActor actor = null;
    final int MAX_RPC_RETRIES = nnList.size();

    for (int i = 0; i <= MAX_RPC_RETRIES; i++) { // min value of MAX_RPC_RETRIES is 0
      try {
        actor = nextNNForNonBlkReportRPC();
        if (actor != null) {
          Object obj = handler.doAction(actor);
          //no exception
          success = true;
          return obj;
        }

      } catch (Exception e) {
        exception = e;
        if (ExceptionCheck.isLocalConnectException(e) ||
                ExceptionCheck.isRetriableException(e)) { //retry
          //black list the namenode
          //so that it is not used again
          LOG.debug("RPC faild. NN used was " + actor.getNNSocketAddress() +
              ", retries left (" + (MAX_RPC_RETRIES - (i)) + ")  Exception " +
              e);

          if( ExceptionCheck.isLocalConnectException(e)) {
            blackListNN.add(actor.getNNSocketAddress());
          }
          continue;
        } else {
          break;
        }
      }
    }

    if (!success) {
      if (exception != null) {
        if (exception instanceof RemoteException) {
          throw (RemoteException) exception;
        }
        else if (exception instanceof ClusterJDatastoreException) {
            LOG.error("Encountered ClusterJDatastoreException exception while storing intermediate block reports " +
                    "in intermediate storage...", exception);
            exception.printStackTrace();
            throw new IOException("Failed to store intermediate block reports in intermediate storage...");
        }
        else {
          throw (IOException) exception;
        }
      }
    }
    return null;
  }

  private  BPServiceActor nextNNForNonBlkReportRPC() {
    readLock();
    try{
    if (nnList == null || nnList.isEmpty()) {
      return null;
    }

      for (int i = 0; i < 10; i++) {
        try {
          rpcRoundRobinIndex.incrementAndGet();
          ActiveNode ann = nnList.get(Math.abs(rpcRoundRobinIndex.get())% nnList.size());
        if (!this.blackListNN.contains(ann.getRpcServerAddressForDatanodes())) {
          BPServiceActor actor = getAnActor(ann.getRpcServerAddressForDatanodes());
            if (actor != null) {
              return actor;
            }
          }
        } catch (Exception e) {
          //any kind of exception try again
          continue;
        }
      }
    return null;
    }finally{
      readUnlock();
    }
  }

  private ActiveNode nextNNForBlkReport(long noOfBlks) throws IOException {
    if (nnList == null || nnList.isEmpty()) {
      return null;
    }

    ActiveNode annToBR = null;
    BPServiceActor leaderActor = getLeaderActor();
    if (leaderActor != null) {
      try {
        annToBR = leaderActor.nextNNForBlkReport(noOfBlks, bpRegistration);
      } catch (RemoteException e) {
        if (e.getClassName().equals(BRLoadBalancingNonLeaderException.class.getName())
                || e.getClassName().equals(BRLoadBalancingOverloadException.class.getName())) {
          LOG.warn(e);

          if(e.getClassName().equals(BRLoadBalancingNonLeaderException.class.getName())){
            //refresh list of active namenodes
            nnListLastUpdate = 0; //This will trigger nnList update
          }

        } else {
          throw e;
        }
      }
    }
    return annToBR;
  }

  private BPServiceActor getLeaderActor() {
    if (nnList.size() > 0) {
      ActiveNode leaderNode = null;
      for (ActiveNode an : nnList) {
        if (leaderNode == null) {
          leaderNode = an;
        }

        if (leaderNode.getId() > an.getId()) {
          leaderNode = an;
        }
      }
      BPServiceActor leaderActor = this.getAnActor(leaderNode.getRpcServerAddressForDatanodes());
      return leaderActor;
    }
    return null;
  }

  private synchronized void forwardRRIndex() {
    readLock();
    try {
      if (nnList != null && !nnList.isEmpty()) {
        // watch out for black listed NN
        for (int i = 0; i < 10; i++) {
          refreshNNRoundRobinIndex.incrementAndGet();
          ActiveNode ann = nnList.get(Math.abs(refreshNNRoundRobinIndex.get()) % nnList.size());
          if (!this.blackListNN.contains(ann.getRpcServerAddressForDatanodes())) {
            return;
          }
        }
      } else {
        refreshNNRoundRobinIndex.set(-1);
      }
    } finally {
      readUnlock();
    }
  }

  private static class PerStoragePendingIncrementalBR {
    private Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
        Maps.newHashMap();
    /**
     * Return the number of blocks on this storage that have pending
     * incremental block reports.
     * @return
     */
    int getBlockInfoCount() {
      return pendingIncrementalBR.size();
    }
    /**
     * Dequeue and return all pending incremental block report state.
     * @return
     */
    ReceivedDeletedBlockInfo[] dequeueBlockInfos() {
      ReceivedDeletedBlockInfo[] blockInfos =
          pendingIncrementalBR.values().toArray(
              new ReceivedDeletedBlockInfo[getBlockInfoCount()]);
      pendingIncrementalBR.clear();
      return blockInfos;
    }
    /**
     * Add blocks from blockArray to pendingIncrementalBR, unless the
     * block already exists in pendingIncrementalBR.
     * @param blockArray list of blocks to add.
     * @return the number of missing blocks that we added.
     */
    int putMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray) {
      int blocksPut = 0;
      for (ReceivedDeletedBlockInfo rdbi : blockArray) {
        if (!pendingIncrementalBR.containsKey(rdbi.getBlock().getBlockId())) {
          pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
          ++blocksPut;
        }
      }
      return blocksPut;
    }
    /**
     * Add pending incremental block report for a single block.
     * @param blockInfo
     */
    void putBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      pendingIncrementalBR.put(blockInfo.getBlock().getBlockId(), blockInfo);
    }
    /**
     * Remove pending incremental block report for a single block if it
     * exists.
     *
     * @param blockInfo
     * @return true if a report was removed, false if no report existed for
     *         the given block.
     */
    boolean removeBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      return (pendingIncrementalBR.remove(blockInfo.getBlock().getBlockId()) != null);
    }
  }
  
  /*
   * Let the actor retry for initialization until all namenodes of cluster have
   * failed.
   */
  boolean shouldRetryInit() {
    if (hasBlockPoolId()) {
      // One of the namenode registered successfully. lets continue retry for
      // other.
      return true;
    }
    return isAlive();
  }
  
  void triggerBlockReport(BlockReportOptions options) throws IOException {
    if (options.isIncremental()) {
      LOG.info(this.toString() + ": scheduling an incremental block report.");
      synchronized(pendingIncrementalBRperStorage) {
        sendImmediateIBR = true;
        pendingIncrementalBRperStorage.notifyAll();
      }
    } else {
      LOG.info(this.toString() + ": scheduling a full block report.");
      synchronized(pendingIncrementalBRperStorage) {
        scheduler.scheduleBlockReport(0);
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }
  
  /**
   * Utility class that wraps the timestamp computations for scheduling
   * heartbeats and block reports.
   */
  static class Scheduler {
    // nextBlockReportTime and nextHeartbeatTime may be assigned/read
    // by testing threads (through BPOfferService#triggerXXX), while also
    // assigned/read by the service thread.
    @VisibleForTesting
    volatile long nextBlockReportTime = monotonicNow();
    
    @VisibleForTesting
    volatile long nextHeartbeatTime = monotonicNow();

    @VisibleForTesting
    boolean resetBlockReportTime = true;

    private final long heartbeatIntervalMs;
    private final long blockReportIntervalMs;
    private boolean nextBlockReportOverwritten = false;

    Scheduler(long heartbeatIntervalMs, long blockReportIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      this.blockReportIntervalMs = blockReportIntervalMs;
    }

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
    
    boolean isBlockReportDue() {
      // TODO: Implement full block reports.
      // LOG.warn("Returning hard-coded 'false' for isBlockReportDue()");
      return false;
      //return nextBlockReportTime - monotonicNow() <= 0;
    }

    /**
     * This methods  arranges for the data node to send the block report at
     * the next heartbeat.
     */
    long scheduleBlockReport(long delay) {
      if (delay > 0) { // send BR after random delay
        // Numerical overflow is possible here and is okay.
        nextBlockReportTime =
            monotonicNow() + DFSUtil.getRandom().nextInt((int) (delay));
      } else { // send at next heartbeat
        nextBlockReportTime = monotonicNow();
      }
      resetBlockReportTime = true; // reset future BRs for randomness
      nextBlockReportOverwritten = true; //make sure that if there is an ongoing blockreport it will not cancel this
      return nextBlockReportTime;
    }

    /**
     * Schedule the next block report after the block report interval. If the
     * current block report was delayed then the next block report is sent per
     * the original schedule.
     * Numerical overflow is possible here.
     */
    void scheduleNextBlockReport() {
    //do not set lastBlockReport when thisone has been set through 
    // scheduleBlockReport and not run yet
    if(nextBlockReportOverwritten){
      nextBlockReportOverwritten = false;
      return;
    }
      // If we have sent the first set of block reports, then wait a random
      // time before we start the periodic block reports.
      if (resetBlockReportTime) {
        nextBlockReportTime = monotonicNow() +
            DFSUtil.getRandom().nextInt((int)(blockReportIntervalMs));
        resetBlockReportTime = false;
      } else {
        /* say the last block report was at 8:20:14. The current report
         * should have started around 9:20:14 (default 1 hour interval).
         * If current time is :
         *   1) normal like 9:20:18, next report should be at 10:20:14
         *   2) unexpected like 11:35:43, next report should be at 12:20:14
         */
        nextBlockReportTime +=
              (((monotonicNow() - nextBlockReportTime + blockReportIntervalMs) /
                  blockReportIntervalMs)) * blockReportIntervalMs;
      }
    }

    void setNextBlockReportOverwritten(boolean value){
      nextBlockReportOverwritten = value;
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
