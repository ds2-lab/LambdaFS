/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.handler;

import io.hops.events.*;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.InvalidationDataAccess;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.Invalidation;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.INodeContext;
import io.hops.transaction.lock.HdfsTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.zookeeper.GuestWatcherOption;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class HopsTransactionalRequestHandler
        extends TransactionalRequestHandler implements HopsEventListener {

  private final String path;

  /**
   * Used to keep track of whether an ACK has been received from each follower NN during the consistency protocol.
   */
  private HashSet<Long> waitingForAcks = new HashSet<>();

  /**
   * Mapping from deployment number to the set of NN IDs, representing the set of ACKs we're still waiting on
   * for that deployment.
   */
  private HashMap<Integer, Set<Long>> waitingForAcksPerDeployment = new HashMap<>();

  /**
   * HashMap from NameNodeID to deployment number, as we can't really recovery deployment number for a given
   * event in the event-received handler. But we can if we note which deployment a given NN is from, because
   * events contain NameNodeID information.
   */
  private HashMap<Long, Integer> nameNodeIdToDeploymentNumberMapping = new HashMap<>();

  /**
   * Used to access the serverless name node instance in the NDB event handler.
   */
  private ServerlessNameNode serverlessNameNodeInstance;

  /**
   * We use this CountDownLatch when waiting on ACKs and watching for changes in membership. Specifically,
   * each time we receive an ACK, the latch is decremented, and if any follower NNs leave the group during
   * this operation, the latch is also decremented. Thus, we are eventually woken up when the CountDownLatch
   * reaches zero.
   */
  private CountDownLatch countDownLatch;

  /**
   * Used to keep track of write ACKs required from each deployment. Normally, we only require ACKs from our own
   * deployment; however, we may require ACKs from other deployments during subtree operations and when creating
   * new directories.
   */
  private Map<Integer, List<WriteAcknowledgement>> writeAcknowledgementsMap;

  /**
   * Set of IDs denoting deployments from which we require ACKs. Our own deployment will always be involved.
   * Other deployments may be involved during subtree operations and when creating new directories, as these
   * types of operations modify INodes from multiple deployments.
   * */
  private Set<Integer> involvedDeployments;

  /**
   * Watchers we create to observe membership changes on other deployments.
   *
   * We remove these when we leave the deployment.
   */
  private final HashMap<Integer, Watcher> watchers;

  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType);
    this.path = path;
    this.watchers = new HashMap<>();
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new HdfsTransactionalLockAcquirer();
  }

  @Override
  public void commitEvents() {
    // If the local `serverlessNameNodeInstance` variable is null, then we try to grab the static instance
    // from the ServerlessNameNode class directly. It is possible that both will be null, however.
    ServerlessNameNode instance = (serverlessNameNodeInstance == null) ?
            ServerlessNameNode.tryGetNameNodeInstance(false) :
            serverlessNameNodeInstance;

    if (instance == null) {
      // requestHandlerLOG.warn("Serverless NameNode instance is null. Cannot commit events directly.");
      // requestHandlerLOG.warn("Committing events to temporary, static variable.");

      ThreadLocal<Set<TransactionEvent>> tempEventsThreadLocal = OpenWhiskHandler.temporaryEventSet;
      Set<TransactionEvent> tempEvents = tempEventsThreadLocal.get();

      if (tempEvents == null) {
        tempEvents = new HashSet<>();
        tempEventsThreadLocal.set(tempEvents);
      }

      tempEvents.add(this.transactionEvent);
    } else {
      String requestId = instance.getRequestCurrentlyProcessing();
      this.transactionEvent.setRequestId(requestId);
      // requestHandlerLOG.debug("Committing transaction event for transaction " + operationId + " now. Associated request ID: " + requestId);
      // This adds to a set, so multiple adds won't mess anything up.
      instance.addTransactionEvent(this.transactionEvent);
    }
  }

//  @Override
//  protected void checkAndHandleNewConcurrentWrites(long txStartTime) throws StorageException {
//    requestHandlerLOG.debug("Checking for concurrent write operations that began after this local one.");
//    WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
//            (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);
//
//    Map<Long, WriteAcknowledgement> mapping =
//            writeAcknowledgementDataAccess.checkForPendingAcks(serverlessNameNodeInstance.getId(), txStartTime);
//
//    if (mapping.size() > 0) {
//      if (mapping.size() == 1)
//        requestHandlerLOG.debug("There is 1 pending ACK for a write operation that began after this local one.");
//      else
//        requestHandlerLOG.debug("There are " + mapping.size() +
//                " pending ACKs for write operation(s) that began after this local one.");
//
//      // Build up a list of INodes (by their IDs) that we need to keep invalidated. They must remain invalidated
//      // because there are future write operations that are writing to them, so we don't want to set their valid
//      // flags to 'true' after this.
//      //
//      // Likewise, any INodes NOT in this list should be set to valid. Consider a scenario where we are the latest
//      // write operation in a series of concurrent/overlapping write operations. In this scenario, the INodes that we
//      // are modifying presumably already had their `INV` flags set to True. As I'm writing this, I'm not entirely sure
//      // if it's possible for us to get a local copy of an INode with an `INV` bit set to true, since eventually read
//      // operations will block and not return INodes with an `INV` column value of true. But in any case, we need to
//      // make sure the INodes we're modifying are valid after this. We locked the rows, so nobody else can change them.
//      // If another write comes along and invalidates them immediately, that's fine. But if we don't ensure they're all
//      // set to valid, then reads may continue to block indefinitely.
//      List<Long> nodesToKeepInvalidated = new ArrayList<Long>();
//      for (Map.Entry<Long, WriteAcknowledgement> entry : mapping.entrySet()) {
//        long operationId = entry.getKey();
//        WriteAcknowledgement ack = entry.getValue();
//
//        requestHandlerLOG.debug("   Operation ID: " + operationId + ", ACK: " + ack.toString());
//
//        nodesToKeepInvalidated.add();
//      }
//    }
//  }
  
  @Override
  protected Object execute(final Object namesystem) throws IOException {
//    if (namesystem instanceof FSNamesystem) {
//      FSNamesystem namesystemInst = (FSNamesystem)namesystem;
//      List<ActiveNode> activeNodes = namesystemInst.getActiveNameNodesInDeployment();
//      requestHandlerLOG.debug("Active nodes: " + activeNodes.toString());
//    } else if (namesystem == null) {
//      requestHandlerLOG.debug("Transaction namesystem object is null! Cannot determine active nodes.");
//    } else {
//      requestHandlerLOG.debug("Transaction namesystem object is of type " + namesystem.getClass().getSimpleName());
//    }

    return super.execute(new TransactionInfo() {
      @Override
      public String getContextName(OperationType opType) {
        if (namesystem instanceof FSNamesystem) {
          return "NN (" + ((FSNamesystem) namesystem).getNamenodeId() + ") " +
              opType.toString() + "[" + Thread.currentThread().getId() + "]";
        } else {
          return opType.toString();
        }
      }

      @Override
      public void performPostTransactionAction() throws IOException {
        if (namesystem instanceof FSNamesystem) {
          ((FSNamesystem) namesystem).performPendingSafeModeOperation();
        }
      }
    });
  }

  @Override
  protected final void preTransactionSetup() throws IOException {
    setUp();
  }

  @Override
  protected final boolean consistencyProtocol(long txStartTime, TransactionAttempt attempt) throws IOException {
    EntityContext<?> inodeContext= EntityManager.getEntityContext(INode.class);
    final boolean[] canProceed = new boolean[1];
    final Throwable[] exception = new Throwable[1];
    Thread protocolRunner = new Thread(() -> {
      try {
        canProceed[0] = doConsistencyProtocol(inodeContext, txStartTime, attempt);
      } catch (IOException e) {
        exception[0] = e;
        canProceed[0] = false;
      }
    }, "ThreadTx" + operationId); // Name the thread with the transaction's ID. This is NOT the requestID.

    protocolRunner.start();

    try {
      protocolRunner.join();
    } catch (InterruptedException ex) {
      throw new IOException("Encountered InterruptedException while waiting for consistency protocol to finish: ", ex);
    }

    boolean proceedWithTx = canProceed[0];

    if (!proceedWithTx) {
      Throwable t = exception[0];

      // If no exception was explicitly thrown, then we'll just say that in the exception that we throw.
      if (t == null)
        throw new IOException("Transaction encountered critical error, but no exception was thrown. Probably timed out.");

      // If there was an exception thrown, then we'll rethrow it as-is if it is an IOException.
      // If it is not an IOException, then we'll wrap it in an IOException.
      if (t instanceof IOException)
        throw (IOException)t;
      else
        throw new IOException("Exception encountered during consistency protocol: " + t.getMessage(), t);
    }

    // 'proceedWithTx' will always be true at this point, since we throw an exception if it is false.
    return true;
  }

  // TODO: Does the consistency protocol step on the subtree protocol? Since setting the subtree lock flag
  //       requires going thru the consistency protocol.
  /**
   * This function should be overridden in order to provide a consistency protocol whenever necessary.
   *
   * We put this in a separate function from
   * {@link HopsTransactionalRequestHandler#consistencyProtocol(long, TransactionAttempt)} as we want it to run it its
   * own thread. If we do not put it in its own thread, then none of the ACK entries or INV entries will actually be
   * added to intermediate storage. The NDB Session object used by the thread performing the transaction is, of course,
   * in an NDB transaction. So, none of the updates would get applied until the tx is committed. This includes ACKs and
   * INVs. Using a separate thread (and therefore a unique, separate Session instance) circumvents this issue.
   *
   * @param entityContext Must be an INodeContext object. Used to determine which INodes are being written to.
   * @param txStartTime The time at which the transaction began. Used to order operations.
   * @param attempt Used to record metrics about the consistency protocol.
   *
   * @return True if the transaction can safely proceed, otherwise false.
   */
  public boolean doConsistencyProtocol(EntityContext<?> entityContext, long txStartTime, TransactionAttempt attempt) throws IOException {
    //// // // // // // // // // // ////
    // CURRENT CONSISTENCY PROTOCOL   //
    //// // // // // // // // // // ////
    //
    // TERMINOLOGY:
    // - Leader NameNode: The NameNode performing the write operation.
    // - Follower NameNode: NameNode instance from the same deployment as the Leader NameNode.
    //
    // The updated consistency protocol for Serverless NameNodes is as follows:
    // (1) The Leader NN begins listening for changes in group membership from ZooKeeper.
    //     The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly. We
    //     need to subscribe first to ensure we receive notifications from follower NNs ACK'ing the entries.
    //     IMPORTANT: Another reason we must subscribe BEFORE adding the ACKs is that, if we add our ACKs first, then
    //     another NN could issue an INV that causes one of our followers to check for ACKs. They see the ACKs we just
    //     added and ACK them, but we haven't subscribed yet! So, we miss the notification, and our protocol fails as
    //     a result. Thus, we must subscribe BEFORE adding any ACKs to intermediate storage.
    // (2) The Leader NN adds N-1 un-ACK'd records to the "ACKs" table of the Leader's deployment, where N is the
    //     number of nodes in the Leader's deployment. (N-1 as it does not need to add a record for itself.)
    // (3) The leader issues one INV per modified INode to the target deployment's INV table.
    // (4) Follower NNs will ACK their entry in the ACKs table upon receiving the INV from intermediate storage (NDB).
    //     The follower will also invalidate its cache at this point, thereby readying itself for the upcoming write.
    // (5) The Leader listens for updates on the ACK table, waiting for all entries to be ACK'd.
    //     If there are any NN failures during this phase, the Leader will detect them via ZK. The Leader does not
    //     need ACKs from failed NNs, as they invalidate their cache upon returning.
    // (6) Once all the "ACK" table entries added by the Leader have been ACK'd by followers, the Leader will check to
    //     see if there are any new, concurrent write operations with a larger timestamp. If so, the Leader must
    //     first finish its own write operation BEFORE submitting any ACKs for those new writes. Then, the leader can
    //     ACK any new write operations that may be waiting.
    //
    //// // // // // // // // // // //// // // // // // // // // // //// // // // // // // // // // ////
    long startTime = System.currentTimeMillis();

    if (!(entityContext instanceof INodeContext))
      throw new IllegalArgumentException("Consistency protocol requires an instance of INodeContext. " +
              "Instead, received " +
              ((entityContext == null) ? "null." : "instance of " +
                      entityContext.getClass().getSimpleName() + "."));

    INodeContext transactionINodeContext = (INodeContext)entityContext;

    Collection<INode> invalidatedINodes = transactionINodeContext.getInvalidatedINodes();
    int numInvalidated = invalidatedINodes.size();

    // If there are no invalidated INodes, then we do not need to carry out the consistency protocol;
    // however, if there is at least 1 invalidated INode, then we must proceed with the protocol.
    if (numInvalidated == 0)
      return true;

    requestHandlerLOG.debug("=-=-=-=-= CONSISTENCY PROTOCOL =-=-=-=-=");
    requestHandlerLOG.debug("Operation ID: " + operationId);
    requestHandlerLOG.debug("Operation Start Time: " + txStartTime);
    serverlessNameNodeInstance = ServerlessNameNode.tryGetNameNodeInstance(true);
    printSuccessMessage = true;

    // Sanity check. Make sure we have a valid reference to the ServerlessNameNode. This isn't the cleanest, but
    // with the way HopsFS has structured its code, this is workable for our purposes.
    if (serverlessNameNodeInstance == null)
      throw new IllegalStateException(
              "Somehow a Transaction is occurring when the static ServerlessNameNode instance is null.");

    transactionEvent.setRequestId(serverlessNameNodeInstance.getRequestCurrentlyProcessing());

    // NOTE: The local deployment will NOT always be involved now that the subtree protocol uses this same code.
    //       Before the subtree protocol used this code, the only NNs that could modify an INode were those from
    //       the mapped deployment. As a result, the Leader NN's deployment would always be involved. But now that the
    //       subtree protocol uses this code, the Leader may not be modifying an INode from its own deployment. So, we
    //       do not automatically add the local deployment to the set of involved deployments. If the local deployment
    //       needs to be invalidated, then it will be added when we see that a modified INode is mapped to it.
    //
    // Keep track of all deployments involved in this transaction.
    involvedDeployments = new HashSet<>();

    for (INode invalidatedINode : invalidatedINodes) {
      int mappedDeploymentNumber = serverlessNameNodeInstance.getMappedServerlessFunction(invalidatedINode);
      int localDeploymentNumber = serverlessNameNodeInstance.getDeploymentNumber();

      // We'll have to guest-join the other deployment if the INode is not mapped to our deployment.
      // This is common during subtree operations and when creating a new directory (as that modifies the parent
      // INode of the new directory, which is possibly mapped to a different deployment).
      if (mappedDeploymentNumber != localDeploymentNumber) {
        requestHandlerLOG.debug("INode '" + invalidatedINode.getLocalName() +
                "' is mapped to a different deployment (" + mappedDeploymentNumber + ").");
        involvedDeployments.add(mappedDeploymentNumber);
      }
    }

    requestHandlerLOG.debug("Leader NameNode: " + serverlessNameNodeInstance.getFunctionName() + ", ID = "
            + serverlessNameNodeInstance.getId() + ", Follower NameNodes: "
            + serverlessNameNodeInstance.getActiveNameNodes().getActiveNodes().toString() + ".");

    // Technically this isn't true yet, but we'll need to unsubscribe after the call to `subscribeToAckEvents()`.
    boolean needToUnsubscribe = true;

    long preprocessingEndTime = System.currentTimeMillis();
    attempt.setConsistencyPreprocessingTimes(startTime, preprocessingEndTime);

    // ======================================
    // === EXECUTING CONSISTENCY PROTOCOL ===
    // ======================================

    // OPTIMIZATION: Pre-calculate the number of write ACK records that we'll be adding to intermediate storage.
    // If this value is zero, we do not need to subscribe to ACK events.
    //
    // As an optimization, we first calculate how many ACKs we're going to need. If we find that this value is 0,
    // then we do not bother subscribing to ACK events. But if there is at least one ACK, then we subscribe first
    // before adding the ACKs to NDB. We need to be listening for ACK events before the events are added so that
    // we do not miss any notifications.
    int totalNumberOfACKsRequired;
    try {
      // Pass the set of additional deployments we needed to join, as we also need ACKs from those deployments.
      totalNumberOfACKsRequired = computeAckRecords(txStartTime);
    } catch (Exception ex) {
      requestHandlerLOG.error("Exception encountered while computing/creating ACK records in-memory:", ex);
      return false;
    }

    long computeAckRecordsEndTime = System.currentTimeMillis();
    attempt.setConsistencyComputeAckRecordTimes(preprocessingEndTime, computeAckRecordsEndTime);

    // =============== STEP 1 ===============
    //
    // We only need to perform these steps of the protocol if the total number of ACKs required is at least one.
    // As an optimization, we split Step 1 into two parts. In the first part, we simply create the ACK records
    // in-memory. Sometimes we won't create any. In that case, we can essentially skip the entire protocol. If we
    // create at least one ACK record in-memory however, then we must first subscribe to ACK events BEFORE adding
    // the newly-created ACK records to intermediate storage. Once we've subscribed to ACK events, we'll write the
    // ACK records to NDB. Doing things in this order eliminates the chance that we miss a notification.
    if (totalNumberOfACKsRequired > 0) {
      // Now that we've added ACKs based on the current membership of the group, we'll join the deployment as a guest
      // and begin monitoring for membership changes. Since we already added ACKs for every active instance, we aren't
      // going to miss any. We DO need to double-check that nobody dropped between when we first queried the deployments
      // for their membership and when we begin listening for changes, though. (We do the same for our own deployment.)
      // joinOtherDeploymentsAsGuest();

      long joinDeploymentsEndTime = System.currentTimeMillis();
      attempt.setConsistencyJoinDeploymentsTimes(computeAckRecordsEndTime, joinDeploymentsEndTime);

      try {
        // Since there's at least 1 ACK record, we subscribe to ACK events. We have not written any ACKs to NDB yet.
        subscribeToAckEvents();

        long subscribeToEventsEndTime = System.currentTimeMillis();
        attempt.setConsistencySubscribeToAckEventsTimes(joinDeploymentsEndTime, subscribeToEventsEndTime);
      } catch (InterruptedException e) {
        requestHandlerLOG.error("Encountered error while waiting on event manager to create event subscription:", e);

        // COMMENTED OUT: Previously, we did things in a different order. But this allowed for a race condition that
        // caused us to miss notifications that our invalidations had been ACK'd. We have since changed the order. As
        // a result, we have not written any ACKs to intermediate storage by this point in the protocol, so there is
        // no need to delete them if we encounter an error.
        //
        // Try to remove the ACK entries that we added earlier before we abort the protocol.
        //try {
        //  requestHandlerLOG.debug("Removing the write ACKs we added earlier before terminating the transaction.");
        //  deleteWriteAcknowledgements();
        //} catch (StorageException ex) {
        //  requestHandlerLOG.error("Encountered storage exception when removing ACK events we added earlier: ", ex);
        //}

        return false;
      }

      long writeAcksToStorageStartTime = System.currentTimeMillis();

      // =============== STEP 2 ===============
      //
      // Now that we've subscribed to ACK events, we can add our ACKs to the table.
      requestHandlerLOG.debug("=-----=-----= Step 2 - Writing ACK Records to Intermediate Storage =-----=-----=");
      WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
              (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);

      for (Map.Entry<Integer, List<WriteAcknowledgement>> entry : writeAcknowledgementsMap.entrySet()) {
        int deploymentNumber = entry.getKey();
        List<WriteAcknowledgement> writeAcknowledgements = entry.getValue();

        if (writeAcknowledgements.size() > 0) {
          requestHandlerLOG.debug("Adding " + writeAcknowledgements.size()
                  + " ACK entries for deployment #" + deploymentNumber + ".");
          writeAcknowledgementDataAccess.addWriteAcknowledgements(writeAcknowledgements, deploymentNumber);
        } else {
          requestHandlerLOG.debug("0 ACKs required from deployment #" + deploymentNumber + "...");
        }
      }

      long writeAcksToStorageEndTime = System.currentTimeMillis();
      attempt.setConsistencyWriteAcksToStorageTimes(writeAcksToStorageStartTime, writeAcksToStorageEndTime);

      // =============== STEP 3 ===============
      issueInitialInvalidations(invalidatedINodes, txStartTime);

      long issueInvalidationsEndTime = System.currentTimeMillis();
      attempt.setConsistencyIssueInvalidationsTimes(writeAcksToStorageEndTime, issueInvalidationsEndTime);

      try {
        // STEP 4 & 5
        waitForAcks();

        long waitForAcksEndTime = System.currentTimeMillis();
        attempt.setConsistencyWaitForAcksTimes(issueInvalidationsEndTime, waitForAcksEndTime);
      } catch (Exception ex) {
        requestHandlerLOG.error("Exception encountered on Step 4 and 5 of consistency protocol (waiting for ACKs).");
        requestHandlerLOG.error("We're still waiting on " + waitingForAcks.size() +
                " ACKs from the following NameNodes: " + waitingForAcks);
        ex.printStackTrace();

        // Clean things up before aborting.
        // TODO: Move this to after we rollback so other reads/writes can proceed immediately without
        //       having to wait for us to clean-up.
        try {
          cleanUpAfterConsistencyProtocol(needToUnsubscribe);
        } catch (Exception e) {
          // We should still be able to continue, despite failing to clean up after ourselves...
          requestHandlerLOG.error("Encountered error while cleaning up after the consistency protocol: ", e);
        }

        // Throw an exception so that it gets caught and reported as an exception, rather than just returning false.
        throw new IOException("Exception encountered while waiting for ACKs (" + ex.getMessage() + "): ", ex);
      }
    }
    else {
      requestHandlerLOG.debug("We do not require any ACKs, so we can skip the rest of the consistency protocol.");
      needToUnsubscribe = false;
    }

    long cleanUpStartTime = System.currentTimeMillis();

    // Clean up ACKs, event operation, etc.
    try {
      cleanUpAfterConsistencyProtocol(needToUnsubscribe);
    } catch (Exception e) {
      // We should still be able to continue, despite failing to clean up after ourselves...
      requestHandlerLOG.error("Encountered error while cleaning up after the consistency protocol: ", e);
    }

    long cleanUpEndTime = System.currentTimeMillis();
    attempt.setConsistencyCleanUpTimes(cleanUpStartTime, cleanUpEndTime);

    // Steps 6 and 7 happen automatically. We can return from this function to perform the writes.
    return true;
  }

  /**
   * This is used as a listener for ZooKeeper events during the consistency protocol. This updates the
   * datastructures tracking the ACKs we're waiting on in response to follower NNs dropping out during the
   * consistency protocol.
   *
   * This function is called once AFTER being set as the event listener to ensure no membership changes occurred
   * between when the leader NN first checked group membership to create the ACK entries and when the leader begins
   * monitoring explicitly for changes in group membership.
   *
   * @param deploymentNumber The deployment number of the given group. Note that the group name is just
   *                         "namenode" + deploymentNumber.
   * @param calledManually Indicates that we called this function manually rather than automatically in response
   *                       to a ZooKeeper event. Really just used for debugging.
   */
  private synchronized void checkAndProcessMembershipChanges(int deploymentNumber, boolean calledManually)
          throws Exception {
    String groupName = "namenode" + deploymentNumber;

    if (calledManually)
      requestHandlerLOG.debug("ZooKeeper detected membership change for group: " + groupName);
    else
      requestHandlerLOG.debug("Checking for membership changes for deployment #" + deploymentNumber);

    ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();

    // Get the current members.
    List<String> groupMemberIdsAsStrings = zkClient.getPermanentGroupMembers(groupName);

    // Convert from strings to longs.
    List<Long> groupMemberIds = groupMemberIdsAsStrings.stream()
            .mapToLong(Long::parseLong)
            .boxed()
            .collect(Collectors.toList());

    requestHandlerLOG.debug("Deployment #" + deploymentNumber + " has " + groupMemberIds.size() +
            " active instance(s): " + StringUtils.join(groupMemberIds, ", "));

    // For each NN that we're waiting on, check that it is still a member of the group. If it is not, then remove it.
    List<Long> removeMe = new ArrayList<>();

    Set<Long> deploymentAcks = waitingForAcksPerDeployment.get(deploymentNumber);

    if (deploymentAcks == null) {
      requestHandlerLOG.debug("We do not require any ACKs from deployment #" + deploymentNumber + ".");
      return;
    }

    requestHandlerLOG.debug("ACKs required from deployment #" + deploymentNumber + ": " +
            StringUtils.join(deploymentAcks, ", "));

    // Compare the group member IDs to the ACKs from JUST this deployment, not the master list of all ACKs from all
    // deployments. If we were to iterate over the master list of all ACKs (that is not partitioned by deployment),
    // then any ACK from another deployment would obviously not be in the groupMemberIds variable, since those group
    // member IDs are just from one particular deployment.
    for (long memberId : deploymentAcks) {
      if (!groupMemberIds.contains(memberId))
        removeMe.add(memberId);
    }

    // Stop waiting on any NNs that have failed since the consistency protocol began.
    if (removeMe.size() > 0) {
      requestHandlerLOG.warn("Found " + removeMe.size()
              + " NameNode(s) that we're waiting on, but are no longer active.");
      requestHandlerLOG.warn("IDs of these NameNodes: " + removeMe);
      removeMe.forEach(s -> {
        waitingForAcks.remove(s);   // Remove from the set of ACKs we're still waiting on.
        deploymentAcks.remove(s);   // Remove from the set of ACKs specific to the deployment.
        countDownLatch.countDown(); // Decrement the count-down latch once for each entry we remove.
      });
    }

    // If after removing all the failed follower NNs, we are not waiting on anybody, then we can just return.
    if (removeMe.size() > 0 && waitingForAcks.size() == 0) {
      requestHandlerLOG.debug("After removal of " + removeMe.size() +
              " failed follower NameNode(s), we have all required ACKs.");
    } else if (removeMe.size() > 0) {
      requestHandlerLOG.debug("After removal of " + removeMe.size() +
              " failed follower NameNode(s), we are still waiting on " + waitingForAcks.size() +
              " more ACK(s) from " + waitingForAcks + ".");
    } else if (waitingForAcks.size() > 0) {
      requestHandlerLOG.debug("No NNs removed from waiting-on ACK list. Still waiting on " + waitingForAcks.size() +
              " more ACK(s) from " + waitingForAcks + ".");
    } else {
      requestHandlerLOG.debug("No NNs removed from waiting-on ACK list. Not waiting on any ACKs.");
    }
  }

  /**
   * This function performs steps 4 and 5 of the consistency protocol. We, as the leader, simply have to wait for the
   * follower NNs to ACK our write operations.
   */
  private void waitForAcks() throws Exception {
    requestHandlerLOG.debug("=-----=-----= Steps 4 & 5 - Waiting for ACKs =-----=-----=");

    ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();
    int localDeploymentNumber = serverlessNameNodeInstance.getDeploymentNumber();

    // Start listening for changes in group membership. We've already added a listener for all deployments
    // that are not our own in the 'joinOtherDeploymentsAsGuest()' function, so this is just for our local deployment.
    zkClient.addListener(serverlessNameNodeInstance.getFunctionName(), watchedEvent -> {
      if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
        try {
          checkAndProcessMembershipChanges(localDeploymentNumber, false);
        } catch (Exception e) {
          requestHandlerLOG.error("Encountered error while reacting to ZooKeeper event.");
          e.printStackTrace();
        }
      }
    });

    // This is a sanity check. For all non-local deployments, there is a small chance there was a membership changed
    // in-between us joining the group/creating ACKs and establishing listeners and all that, so this makes sure
    // our global image of deployment membership is correct. Likewise, there is a chance that membership for our local
    // deployment has changed in between when we created the ACK entries and when we added the ZK listener just now.
    for (int deploymentNumber : involvedDeployments)
      checkAndProcessMembershipChanges(deploymentNumber, true);

    requestHandlerLOG.debug("Waiting for the remaining " + waitingForAcks.size() +
            " ACK(s) now. Will timeout after " + serverlessNameNodeInstance.getTxAckTimeout() + " milliseconds.");
    requestHandlerLOG.debug("Count value of CountDownLatch: " + countDownLatch.getCount());

    // Wait until we're done. If the latch is already at zero, then this will not block.
    boolean success;
    try {
      success = countDownLatch.await(serverlessNameNodeInstance.getTxAckTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted waiting for ACKs from other NameNodes. Waiting on a total of " +
              waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
    }

    if (!success) {
      requestHandlerLOG.warn("Timed out while waiting for ACKs from other NNs. Waiting on a total of " +
              waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
      requestHandlerLOG.debug("Checking liveliness of NNs that we're still waiting on...");

      // If we timed-out, verify that the NameNodes we're waiting on are still, in fact, alive.
      for (int deployment : involvedDeployments) {
        Set<Long> waitingOnInDeployment = waitingForAcksPerDeployment.get(deployment);

        if (waitingOnInDeployment == null)
          continue;

        for (long nameNodeId : waitingOnInDeployment) {
          boolean isAlive = zkClient.checkForPermanentGroupMember(deployment, Long.toString(nameNodeId));

          if (!isAlive) {
            requestHandlerLOG.warn("NN " + nameNodeId + " is no longer alive, yet we're still waiting on them.");
            waitingForAcks.remove(nameNodeId);
          } else {
            requestHandlerLOG.error("NN " + nameNodeId + " is still alive, but has not ACK'd for some reason.");
          }
        }
      }

      if (waitingForAcks.size() == 0) {
        requestHandlerLOG.warn("There are no unreceived ACKs after checking liveliness of other NNs.");
      } else {
        requestHandlerLOG.error("There are still unreceived ACKs after checking liveliness of other NNs.");
        throw new IOException("Timed out while waiting for ACKs from other NameNodes. Waiting on a total of " +
                waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
      }
    }

    assert(waitingForAcks.isEmpty());
    requestHandlerLOG.debug("We have received all required ACKs for write operation " + operationId + ".");
  }

  /**
   * Perform any necessary clean-up steps after the consistency protocol has completed.
   * This includes unsubscribing from ACK table events, removing the ACK entries from the table in NDB, leaving any
   * deployments that we joined as a guest, etc.
   *
   * @param needToUnsubscribe If true, then we still need to unsubscribe from ACK events. If false, then we
   *                          already unsubscribed from ACK events (presumably because we found that we didn't
   *                          actually need any ACKs and just unsubscribed immediately).
   */
  private void cleanUpAfterConsistencyProtocol(boolean needToUnsubscribe)
          throws Exception {
    requestHandlerLOG.debug("Performing clean-up procedure for consistency protocol now.");
    // Unsubscribe and unregister event listener if we haven't done so already. (If we were the only active NN in
    // our deployment at the beginning of the protocol, then we would have already unsubscribed by this point.)
    if (needToUnsubscribe)
      unsubscribeFromAckEvents();

    // leaveDeployments();

    deleteWriteAcknowledgements();

    // TODO: Delete INVs as well?
  }

  /**
   * Delete the write acknowledgement entries we created during the consistency protocol.
   */
  private void deleteWriteAcknowledgements() throws StorageException {
    // Remove the ACK entries that we added.
    WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
            (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);

    // Remove the ACKs we created for each deployment involved in this tranaction.
    for (Map.Entry<Integer, List<WriteAcknowledgement>> entry : writeAcknowledgementsMap.entrySet()) {
      int deploymentNumber = entry.getKey();
      List<WriteAcknowledgement> writeAcknowledgements = entry.getValue();

      if (writeAcknowledgements.size() == 1)
        requestHandlerLOG.debug("Removing 1 ACK entry for deployment #" + deploymentNumber);
      else
        requestHandlerLOG.debug("Removing " + writeAcknowledgements.size() +
                " ACK entries for deployment #" + deploymentNumber);

      writeAcknowledgementDataAccess.deleteAcknowledgements(writeAcknowledgements, deploymentNumber);
    }
  }

  /**
   * Leave deployments that we joined as a guest.
   */
  private void leaveDeployments() throws Exception {
    ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();

    String memberId = serverlessNameNodeInstance.getId() + "-" + Thread.currentThread().getId();
    for (int deployentNumber : involvedDeployments) {
      if (deployentNumber == serverlessNameNodeInstance.getDeploymentNumber())
        continue;

      zkClient.leaveGroup("namenode" + deployentNumber, memberId, false);
      zkClient.removeListener("namenode" + deployentNumber, this.watchers.get(deployentNumber));
    }
  }

  /**
   * Unregister ourselves as an event listener for ACK table events.
   */
  private void unsubscribeFromAckEvents()
          throws StorageException {
    for (int deploymentNumber : involvedDeployments) {
      String eventName = HopsEvent.ACK_EVENT_NAME_BASE + deploymentNumber;
      EventManager eventManager = serverlessNameNodeInstance.getNdbEventManager();

      // This returns a semaphore that we could use to wait, but we don't really care when the operation gets dropped.
      // We just don't care to receive events anymore. We can continue just fine if we receive them for a bit.
      eventManager.requestDropSubscription(eventName, this);
    }
  }

  @Override
  public void eventReceived(HopsEventOperation eventData, String eventName) {
    if (!eventName.contains(HopsEvent.ACK_EVENT_NAME_BASE)) {
      requestHandlerLOG.error("HopsTransactionalRequestHandler received unexpected event " + eventName + "!");
      return;
    }

    String eventType = eventData.getEventType();
    if (eventType.equals(HopsEventType.INSERT)) // We don't care about INSERT events.
      return;

    // First, verify that this event pertains to our write operation. If it doesn't, we just return.
    long writeOpId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.OPERATION_ID);
    long nameNodeId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.NAME_NODE_ID);
    boolean acknowledged = eventData.getBooleanPostValue(TablesDef.WriteAcknowledgementsTableDef.ACKNOWLEDGED);
    int mappedDeployment = nameNodeIdToDeploymentNumberMapping.get(nameNodeId);

    if (writeOpId != operationId) // If it is for a different write operation, then we don't care about it.
      return;

    if (acknowledged) {
      // It's possible that there are multiple transactions going on simultaneously, so we may receive ACKs for
      // NameNodes that we aren't waiting on. We just ignore these.
      // TODO: May want to verify, in general, that we aren't actually receiving too many ACKs, like routinely
      //       verify that there isn't a bug causing NameNodes to ACK the same entry several times.
      if (!waitingForAcks.contains(nameNodeId))
        return;

      requestHandlerLOG.debug("Received ACK from NameNode " + nameNodeId + " (deployment = " +
              mappedDeployment + ")!");

      waitingForAcks.remove(nameNodeId);

      Set<Long> deploymentAcks = waitingForAcksPerDeployment.get(mappedDeployment);
      deploymentAcks.remove(nameNodeId);

      countDownLatch.countDown();
    }
  }

  /**
   * Join other deployments as a guest. This is required when modifying INodes from other deployments. Typically, we
   * aim to avoid this. But it occurs commonly during subtree operations and when creating new directories.
   */
  private void joinOtherDeploymentsAsGuest() throws IOException {
    requestHandlerLOG.debug("There are potentially " + involvedDeployments.size() + " other deployments to join: " +
            involvedDeployments);

    ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();
    long localNameNodeId = serverlessNameNodeInstance.getId();

    // Join the other deployments as a guest, registering a membership-changed listener.
    int counter = 1;
    for (int deploymentNumber : involvedDeployments) {
      // We do not need to join our own deployment; we're already in our own deployment.
      if (deploymentNumber == serverlessNameNodeInstance.getDeploymentNumber())
        continue;

      requestHandlerLOG.debug("Joining deployment " + deploymentNumber + " as guest (" + counter + "/" +
              involvedDeployments.size() + ").");
      final String groupName = "namenode" + deploymentNumber;

      // During subtree transactions, the NameNode may use a large number of threads to concurrently operate on
      // different parts of the subtree. As a result, the NN may try to guest-join the ZooKeeper group multiple
      // times. For now, we just append the thread ID to the NameNode's ID to use as our ZooKeeper ID for the guest
      // group. This is a little messy, but nobody references the guest group anyway. At some point, if we never
      // begin using it, we could just eliminate the behavior (i.e., NameNodes would not explicitly join as a guest).
      String memberId = localNameNodeId + "-" + Thread.currentThread().getId();

      Watcher membershipChangedWatcher = watchedEvent -> {
        // This specifically monitors for NNs leaving the group, rather than joining. NNs that join will have
        // empty caches, so we do not need to worry about them.
        if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
          try {
            // We call this again in waitForAcks() as a sanity check to make sure we haven't missed anything.
            checkAndProcessMembershipChanges(deploymentNumber, false);
          } catch (Exception e) {
            requestHandlerLOG.error("Encountered error while reacting to ZooKeeper event.");
            e.printStackTrace();
          }
        }
        // We only want to monitor the permanent sub-group for membership changes.
      };

      this.watchers.put(deploymentNumber, membershipChangedWatcher);

      try {
        // Join the group.
        zkClient.joinGroupAsGuest("namenode" + deploymentNumber, memberId, membershipChangedWatcher,
                GuestWatcherOption.CREATE_WATCH_ON_PERMANENT);
      } catch (Exception e) {
        throw new IOException("Exception encountered while guest-joining group " + groupName + ":", e);
      }
    }
  }

  /**
   * Return the table name to subscribe to for ACK events, given the deployment number.
   * @param deploymentNumber Deployment number for which a subscription should be created for the associated table.
   * @return The name of the table for which an event subscription should be created.
   * @throws StorageException If the deployment number refers to a non-existent deployment.
   */
  private String getTargetTableName(int deploymentNumber) throws StorageException {
    switch (deploymentNumber) {
      case 0:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME0;
      case 1:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME1;
      case 2:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME2;
      case 3:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME3;
      case 4:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME4;
      case 5:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME5;
      case 6:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME6;
      case 7:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME7;
      case 8:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME8;
      case 9:
        return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME9;
      default:
        throw new StorageException("Unsupported deployment number: " + deploymentNumber);
    }
  }

  /**
   * Perform Step (1) of the consistency protocol:
   *    The Leader NN begins listening for changes in group membership from ZooKeeper.
   *    The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly.
   */
  private void subscribeToAckEvents()
          throws StorageException, InterruptedException {
    requestHandlerLOG.debug("=-----=-----= Step 1 - Subscribing to ACK Events =-----=-----=");

    // Each time we request that the event manager create an event subscription for us, it returns a semaphore
    // we can use to block until the event operation is created. We want to do this here, as we do not want
    // to continue until we know we'll receive the event notifications.
    List<EventRequestSignaler> eventRequestSignalers = new ArrayList<>();

    EventManager eventManager = serverlessNameNodeInstance.getNdbEventManager();
    for (int deploymentNumber : involvedDeployments) {
      String targetTableName = getTargetTableName(deploymentNumber);
      String eventName = HopsEvent.ACK_EVENT_NAME_BASE + deploymentNumber;
      EventRequestSignaler eventRequestSignaler = eventManager.requestCreateEvent(eventName, targetTableName,
              eventManager.getAckTableEventColumns(), false, true,
              this, eventManager.getAckEventTypeIDs());
      eventRequestSignalers.add(eventRequestSignaler);
    }

    requestHandlerLOG.debug("Acquiring " + eventRequestSignalers.size() + " semaphore(s) now.");

    for (EventRequestSignaler eventRequestSignaler : eventRequestSignalers) {
      eventRequestSignaler.waitForCriteria();
    }

    requestHandlerLOG.debug("Successfully acquired " + eventRequestSignalers.size() + " semaphore(s).");
  }

  /**
   * Perform Step (1) of the consistency protocol:
   *    Create the ACK instances that we will add to NDB. We do NOT add them in this function; we merely compute
   *    them (i.e., create the objects). In some cases, we will find that we don't create any, in which case we
   *    skip subscribing to the ACK events table. If we create at least one ACK object in this method however, we
   *    will first subscribe to ACK events, then we will add the ACKs to intermediate storage.
   *
   * @param txStartTime The UTC timestamp at which this write operation began.
   *
   * @return The number of ACK records that we added to intermediate storage.
   */
  private int computeAckRecords(long txStartTime)
          throws Exception {
    requestHandlerLOG.debug("=-----=-----= Step 0 - Pre-Compute ACK Records In-Memory =-----=-----=");
    ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();
    assert(zkClient != null);

    // Sum the number of ACKs required per deployment. We use this value when creating the
    // CountDownLatch that blocks us from continuing with the protocol until all ACKs are received.
    int totalNumberOfACKsRequired = 0;

    writeAcknowledgementsMap = new HashMap<>();

    // If there are no active instances in the deployments that are theoretically involved, then we just remove
    // them from the set of active deployments, as we don't need ACKs from them, nor do we need to store any INVs.
    Set<Integer> toRemove = new HashSet<>();

    // For each deployment (which at least includes our own), get the current members and register a membership-
    // changed listener. This enables us to monitor for any changes in group membership; in particular, we will
    // receive notifications if any NameNodes leave a deployment.
    for (int deploymentNumber : involvedDeployments) {
      List<WriteAcknowledgement> writeAcknowledgements = new ArrayList<>();
      final String groupName = "namenode" + deploymentNumber;
      List<String> groupMemberIds = zkClient.getPermanentGroupMembers(groupName);
      Set<Long> acksForCurrentDeployment = waitingForAcksPerDeployment.getOrDefault(deploymentNumber, null);

      if (acksForCurrentDeployment == null) {
        acksForCurrentDeployment = new HashSet<>();
        waitingForAcksPerDeployment.put(deploymentNumber, acksForCurrentDeployment);
      }

      if (groupMemberIds.size() == 1)
        requestHandlerLOG.debug("There is 1 active instance in deployment #" + deploymentNumber +
                " at the start of consistency protocol: " + groupMemberIds.get(0) + ".");
      else
        requestHandlerLOG.debug("There are " + groupMemberIds.size() + " active instances in deployment #" +
            deploymentNumber + " at the start of consistency protocol: " +
                StringUtils.join(groupMemberIds, ", "));

      if (groupMemberIds.size() == 0) {
        toRemove.add(deploymentNumber);
        continue;
      }

      // Iterate over all the current group members. For each group member, we create a WriteAcknowledgement object,
      // which we'll persist to intermediate storage. We skip ourselves, as we do not need to ACK our own write. We also
      // create an entry for each follower NN in the `writeAckMap` to keep track of whether they've ACK'd their entry.
      for (String memberIdAsString : groupMemberIds) {
        long memberId = Long.parseLong(memberIdAsString);
        nameNodeIdToDeploymentNumberMapping.put(memberId, deploymentNumber); // Note which deployment this NN is from.

        // We do not need to add an entry for ourselves.
        if (memberId == serverlessNameNodeInstance.getId())
          continue;

        waitingForAcks.add(memberId);
        acksForCurrentDeployment.add(memberId);
        writeAcknowledgements.add(new WriteAcknowledgement(memberId, deploymentNumber, operationId,
                false, txStartTime, serverlessNameNodeInstance.getId()));
      }

      writeAcknowledgementsMap.put(deploymentNumber, writeAcknowledgements);
      totalNumberOfACKsRequired += writeAcknowledgements.size();
    }

    for (Integer deploymentToRemove : toRemove) {
      requestHandlerLOG.debug("Removing deployment #" + deploymentToRemove +
              " from list of involved deployments as deployment #" + deploymentToRemove +
              " contains zero active instances.");
      involvedDeployments.remove(deploymentToRemove);
    }

    requestHandlerLOG.debug("Grand total of " + totalNumberOfACKsRequired + " ACKs required.");

    // Instantiate the CountDownLatch variable. The value is set to the number of ACKs that we need
    // before we can proceed with the transaction. Receiving an ACK and a follower NN leaving the group
    // will trigger a decrement.
    countDownLatch = new CountDownLatch(totalNumberOfACKsRequired);

    // This will be zero if we are the only active NameNode.
    return totalNumberOfACKsRequired;
  }

  /**
   * Perform Step (3) of the consistency protocol:
   *    The leader sets the INV flag of the target INode to 1 (i.e., true), thereby triggering a round of
   *    INVs from intermediate storage (NDB).
   *
   * @param invalidatedINodes The INodes involved in this write operation. We must invalidate these INodes.
   * @param txStartTime The time at which the transaction began.
   */
  private void issueInitialInvalidations(Collection<INode> invalidatedINodes, long txStartTime)
          throws StorageException {
    requestHandlerLOG.debug("=-----=-----= Step 3 - Issuing Initial Invalidations =-----=-----=");

    InvalidationDataAccess<Invalidation> dataAccess =
            (InvalidationDataAccess<Invalidation>)HdfsStorageFactory.getDataAccess(InvalidationDataAccess.class);

    Map<Integer, List<Invalidation>> invalidationsMap = new HashMap<>();

    for (INode invalidatedINode : invalidatedINodes) {
      int mappedDeploymentNumber = serverlessNameNodeInstance.getMappedServerlessFunction(invalidatedINode);
      List<Invalidation> invalidations = invalidationsMap.getOrDefault(mappedDeploymentNumber, null);

      if (invalidations == null) {
        invalidations = new ArrayList<>();
        invalidationsMap.put(mappedDeploymentNumber, invalidations);
      }

      // int inodeId, int parentId, long leaderNameNodeId, long txStartTime, long operationId
      invalidations.add(new Invalidation(invalidatedINode.getId(), invalidatedINode.getParentId(),
              serverlessNameNodeInstance.getId(), txStartTime, operationId));
    }

    for (Map.Entry<Integer, List<Invalidation>> entry : invalidationsMap.entrySet()) {
      int deploymentNumber = entry.getKey();
      List<Invalidation> invalidations = entry.getValue();

      int numInvalidations = invalidations.size();
      if (numInvalidations == 0) {
        requestHandlerLOG.debug("Adding 0 INV entries for deployment #" + deploymentNumber + ".");
        continue;
      }
      else if (numInvalidations == 1)
        requestHandlerLOG.debug("Adding 1 INV entry for deployment #" + deploymentNumber + ".");
      else
        requestHandlerLOG.debug("Adding " + numInvalidations + " INV entries for deployment #" +
                deploymentNumber + ".");

      dataAccess.addInvalidations(invalidations, deploymentNumber);
    }
  }

  public void setUp() throws IOException {

  }

  @Override
  protected final boolean shouldAbort(Exception e) {
    if (e instanceof RecoveryInProgressException.NonAbortingRecoveryInProgressException) {
      return false;
    }
    return true;
  }
}
