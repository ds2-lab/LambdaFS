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

import com.esotericsoftware.minlog.Log;
import com.logicalclocks.shaded.org.checkerframework.checker.units.qual.C;
import io.hops.events.EventManager;
import io.hops.events.HopsEvent;
import io.hops.events.HopsEventListener;
import io.hops.events.HopsEventOperation;
import io.hops.exception.StorageException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.INodeContext;
import io.hops.transaction.lock.HdfsTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.invoking.OpenWhiskInvoker;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public abstract class HopsTransactionalRequestHandler
        extends TransactionalRequestHandler implements HopsEventListener {

  private final String path;

  /**
   * Used to keep track of whether an ACK has been received from each follower NN during the consistency protocol.
   */
  private HashSet<Long> waitingForAcks = new HashSet<>();

  /**
   * Used as a unique identifier for the operation. This is only used during write operations.
   */
  private final long operationId;

  /**
   * We use this CountDownLatch when waiting on ACKs and watching for changes in membership. Specifically,
   * each time we receive an ACK, the latch is decremented, and if any follower NNs leave the group during
   * this operation, the latch is also decremented. Thus, we are eventually woken up when the CountDownLatch
   * reaches zero.
   */
  private CountDownLatch countDownLatch;

  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType);
    this.path = path;
    this.operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new HdfsTransactionalLockAcquirer();
  }
  
  @Override
  protected Object execute(final Object namesystem) throws IOException {
//    if (opType.shouldUseConsistencyProtocol()) {
//      requestHandlerLOG.debug("Transaction type <" + opType.getName()
//              + "> *SHOULD* use the serverless consistency protocol.");
//    } else {
//      requestHandlerLOG.debug("Transaction type <" + opType.getName()
//              + "> does NOT need to use the serverless consistency protocol.");
//    }
//    requestHandlerLOG.debug("Transaction is operating on path: " + path);
    if (namesystem instanceof FSNamesystem) {
      FSNamesystem namesystemInst = (FSNamesystem)namesystem;
      List<ActiveNode> activeNodes = namesystemInst.getActiveNameNodesInDeployment();
      requestHandlerLOG.debug("Active nodes: " + activeNodes.toString());
    } else if (namesystem == null) {
      requestHandlerLOG.debug("Transaction namesystem object is null! Cannot determine active nodes.");
    } else {
      requestHandlerLOG.debug("Transaction namesystem object is of type " + namesystem.getClass().getSimpleName());
    }

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
  protected final boolean consistencyProtocol() throws IOException {
    EntityContext<?> inodeContext = EntityManager.getEntityContext(INode.class);
    return doConsistencyProtocol(inodeContext);
  }

  /**
   * This function should be overridden in order to provide a consistency protocol whenever necessary.
   *
   * @return True if the transaction can safely proceed, otherwise false.
   */
  public boolean doConsistencyProtocol(EntityContext<?> entityContext) throws IOException {
    //// // // // // // // // // // ////
    // UPDATED CONSISTENCY PROTOCOL   //
    //// // // // // // // // // // ////
    //
    // TERMINOLOGY:
    // - Leader NameNode: The NameNode performing the write operation.
    // - Follower NameNode: NameNode instance from the same deployment as the Leader NameNode.
    //
    // The updated consistency protocol for Serverless NameNodes is as follows:
    // (1) The leader sets the INV flag of the target INode(s) to 1 (i.e., true), thereby triggering a round of INVs
    //     from intermediate storage (NDB). We have to invalidate the node first so that nobody can read and cache
    //     the node. If we were to add the ACK entries to the table BEFORE invalidating, a new NN could start up,
    //     read the soon-to-be invalidated target INode before we invalidate it, screwing up the whole protocol.
    // (2) The Leader NN begins listening for changes in group membership from ZooKeeper.
    //     The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly. We
    //     need to subscribe first to ensure we receive notifications from follower NNs ACK'ing the entries. Since we
    //     invalidate the INode first, the followers may check for their ACKs before they're available, in which case
    //     they'll retry until we add the ACKs. So they may invalidate the ACK entries right away, meaning we need to
    //     be subscribed from the very beginning.
    // (3) Add N-1 un-ACK'd records to the "ACKs" table, where N is the number of nodes in the Leader's deployment.
    //     (The Leader adds N-1 as it does not need to add a record for itself.)
    // (4) Follower NNs will ACK their entry in the ACKs table upon receiving the INV from intermediate storage (NDB).
    //     The follower will also invalidate its cache at this point, thereby readying itself for the upcoming write.
    // (5) The Leader listens for updates on the ACK table, waiting for all entries to be ACK'd.
    //     If there are any NN failures during this phase, the Leader will detect them via ZK. The Leader does not
    //     need ACKs from failed NNs, as they invalidate their cache upon returning.
    // (6) Once all the "ACK" table entries added by the Leader have been ACK'd by followers, the Leader will check to
    //     see if there are any new, concurrent write operations with a larger timestamp. If so, the Leader must
    //     complete the next step FIRST before submitting any ACKs for those new writes.
    // (7) The Leader will perform the "true" write operation to intermediate storage (NDB). Then, it can ACK any
    //     new write operations that may be waiting.
    // (8) Follower NNs will lazily update their caches on subsequent read operations.
    //
    //// // // // // // // // // // //// // // // // // // // // // //// // // // // // // // // // ////

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

    // Record the time that the write operation started.
    long writeStartTime = Time.getUtcTime();

    requestHandlerLOG.debug("=-=-=-=-= CONSISTENCY PROTOCOL =-=-=-=-=");
    requestHandlerLOG.debug("Operation ID: " + operationId);
    requestHandlerLOG.debug("Operation Start Time: " + writeStartTime);
    ServerlessNameNode serverlessNameNode = OpenWhiskHandler.instance;

    // Sanity check. Make sure we have a valid reference to the ServerlessNameNode. This isn't the cleanest, but
    // with the way HopsFS has structured its code, this is workable for our purposes.
    if (serverlessNameNode == null)
      throw new IllegalStateException(
              "Somehow a Transaction is occurring when the static ServerlessNameNode instance is null.");

    // Sanity check. Make sure we're only modifying INodes that we are authorized to modify.
    // If we find that we are about to modify an INode for which we are not authorized, throw an exception.
    for (INode invalidatedINode : invalidatedINodes) {
      int mappedDeploymentNumber = serverlessNameNode.getMappedServerlessFunction(invalidatedINode);
      int localDeploymentNumber = serverlessNameNode.getDeploymentNumber();

      if (mappedDeploymentNumber != localDeploymentNumber) {
        requestHandlerLOG.error("Transaction intends to update INode " + invalidatedINode.getFullPathName()
                + ", however only NameNodes from deployment #" + mappedDeploymentNumber
                + " should be modifying this INode. We are from deployment #" + localDeploymentNumber);
        throw new IOException("Modification of INode " + invalidatedINode.getFullPathName()
                + " is unauthorized for NameNodes from deployment #" + localDeploymentNumber
                + "; only NameNodes from deployment #" + mappedDeploymentNumber + " may modify this INode.");
      } else {
        requestHandlerLOG.debug("Modification of INode " + invalidatedINode.getFullPathName() + " is permitted.");
      }
    }

    requestHandlerLOG.debug("Leader NameNode: " + serverlessNameNode.getFunctionName() + ", ID = "
            + serverlessNameNode.getId() + ", Follower NameNodes: "
            + serverlessNameNode.getActiveNameNodes().getActiveNodes().toString() + ".");

    // Technically this isn't true yet, but we'll need to unsubscribe after the call to `subscribeToAckEvents()`.
    boolean needToUnsubscribe = true;

    // Carry out the consistency protocol.
    // STEP 1
    issueInitialInvalidations(invalidatedINodes);

    // STEP 2
    subscribeToAckEvents(serverlessNameNode);

    // STEP 3
    List<WriteAcknowledgement> writeAcknowledgements;
    try {
      writeAcknowledgements = addAckTableRecords(serverlessNameNode, writeStartTime);
    } catch (Exception ex) {
      requestHandlerLOG.error("Exception encountered on Step 3 of consistency protocol (adding ACKs to table).");
      ex.printStackTrace();
      return false;
    }

    // If it turns out there are no other active NNs in our deployment, then we can just unsubscribe right away.
    if (writeAcknowledgements.size() == 0) {
      requestHandlerLOG.debug("We're the only active NN in our deployment. Unsubscribing from ACK events now.");
      unsubscribeFromAckEvents(serverlessNameNode);
      needToUnsubscribe = false;
    }


    try {
      // STEP 4 & 5
      waitForAcks(serverlessNameNode);
    } catch (Exception ex) {
      requestHandlerLOG.error("Exception encountered on Step 4 and 5 of consistency protocol (waiting for ACKs).");
      requestHandlerLOG.error("We're still waiting on " + waitingForAcks.size() +
              " ACKs from the following NameNodes: " + waitingForAcks);
      ex.printStackTrace();
      return false;
    }

    // Clean up ACKs, event operation, etc.
    cleanUpAfterConsistencyProtocol(serverlessNameNode, needToUnsubscribe, writeAcknowledgements);

    return true;
  }

  /**
   * This function performs steps 4 and 5 of the consistency protocol. We, as the leader, simply have to wait for the
   * follower NNs to ACK our write operations.
   */
  private void waitForAcks(ServerlessNameNode serverlessNameNode) throws Exception {
    ZKClient zkClient = serverlessNameNode.getZooKeeperClient();

    // Get the current members.
    List<String> groupMemberIdsAsStrings = zkClient.getGroupMembers(serverlessNameNode.getFunctionName());

    // Convert from strings to longs.
    List<Long> groupMemberIds = groupMemberIdsAsStrings.stream()
            .mapToLong(Long::parseLong)
            .boxed()
            .collect(Collectors.toList());

    // For each NN that we're waiting on, check that it is still a member of the group. If it is not, then remove it.
    List<Long> removeMe = new ArrayList<>();
    for (long memberId : waitingForAcks) {
      if (!groupMemberIds.contains(memberId))
        removeMe.add(memberId);
    }

    // Stop waiting on any NNs that have failed since the consistency protocol began.
    if (removeMe.size() > 0) {
      requestHandlerLOG.warn("Found " + removeMe.size()
              + " NameNode(s) that we are waiting on, but are no longer part of the group.");
      requestHandlerLOG.warn("IDs of these NameNodes: " + removeMe);
      removeMe.forEach(waitingForAcks::remove);
    }

    // If after removing all the failed follower NNs, we are not waiting on anybody, then we can just return.
    if (waitingForAcks.size() == 0) {
      requestHandlerLOG.debug("After removal of failed follower NameNodes, we have all required ACKs.");
      return;
    } else {
      requestHandlerLOG.debug("We are still waiting on " + waitingForAcks.size() +
              " more ACK(s) from " + waitingForAcks + ".");
    }

    // Wait until we're done.
    countDownLatch.await();
    requestHandlerLOG.debug("We have received all required ACKs for write operation " + operationId + ".");
  }

  /**
   * Perform any necessary clean-up steps after the consistency protocol has completed.
   * This includes unsubscribing from ACK table events, removing the ACK entries from the table in NDB, etc.
   *
   * TODO: How does the 'INV' flag get reset? It might be the case that the data we intend to write during the
   *       transaction will have the 'INV' configured to be false (so the data is valid), in which case it happens
   *       automatically.
   */
  private void cleanUpAfterConsistencyProtocol(ServerlessNameNode serverlessNameNode, boolean needToUnsubscribe,
                                               Collection<WriteAcknowledgement> writeAcknowledgements)
          throws StorageException {
    // Unsubscribe and unregister event listener if we haven't done so already. (If we were the only active NN in
    // our deployment at the beginning of the protocol, then we would have already unsubscribed by this point.)
    if (needToUnsubscribe)
      unsubscribeFromAckEvents(serverlessNameNode);

    // Remove the ACK entries that we added.
    WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
            (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);
    writeAcknowledgementDataAccess.deleteAcknowledgements(writeAcknowledgements);
  }

  /**
   * Unregister ourselves as an event listener for ACK table events, then unregister the event operation itself.
   */
  private void unsubscribeFromAckEvents(ServerlessNameNode serverlessNameNode) throws StorageException {
    EventManager eventManager = serverlessNameNode.getNdbEventManager();
    eventManager.removeListener(this, HopsEvent.ACK_TABLE_EVENT_NAME);
    eventManager.unregisterEventOperation(HopsEvent.ACK_TABLE_EVENT_NAME);
  }

  @Override
  public void eventReceived(HopsEventOperation eventData, String eventName) {
    if (!eventName.equals(HopsEvent.ACK_TABLE_EVENT_NAME))
      requestHandlerLOG.debug("HopsTransactionalRequestHandler received unexpected event " + eventName + "!");

    // First, verify that this event pertains to our write operation. If it doesn't, we just return.
    long writeId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.OPERATION_ID);
    if (writeId != operationId)
      return;

    long nameNodeId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.NAME_NODE_ID);
    boolean acknowledged =
            eventData.getBooleanPostValue(TablesDef.WriteAcknowledgementsTableDef.ACKNOWLEDGED);

    if (acknowledged) {
      requestHandlerLOG.debug("Received ACK from NameNode " + nameNodeId + "!");

      // If we're receiving an ACK for this NameNode, then it better be the case that
      // we're waiting on it. Otherwise, something is wrong.
      if (!waitingForAcks.contains(nameNodeId))
        throw new IllegalStateException("We received an ACK from NN " + nameNodeId +
                ", but that NN is not in our 'waiting on' list. Size of list: " + waitingForAcks.size() + ".");

      waitingForAcks.remove(nameNodeId);

      countDownLatch.countDown();
    }

  }

  /**
   * Perform Step (2) of the consistency protocol:
   *    The Leader NN begins listening for changes in group membership from ZooKeeper.
   *    The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly.
   */
  private void subscribeToAckEvents(ServerlessNameNode serverlessNameNode) throws StorageException {
    requestHandlerLOG.debug("=-----=-----= Step 3 - Subscribing to ACK Events =-----=-----=");

    EventManager eventManager = serverlessNameNode.getNdbEventManager();
    boolean eventCreated = eventManager.registerEvent(HopsEvent.ACK_TABLE_EVENT_NAME, TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME,
            eventManager.getAckTableEventColumns(), false);

    if (eventCreated)
      requestHandlerLOG.debug("Event " + HopsEvent.ACK_TABLE_EVENT_NAME + " created successfully.");
    else
      requestHandlerLOG.debug("Event " + HopsEvent.ACK_TABLE_EVENT_NAME
              + " already exists. Reusing existing event.");

    eventManager.createEventOperation(HopsEvent.ACK_TABLE_EVENT_NAME);
    eventManager.addListener(this, HopsEvent.ACK_TABLE_EVENT_NAME);
  }

  /**
   * Perform Step (1) of the consistency protocol:
   *    Add N-1 un-ACK'd records to the "ACKs" table, where N is the number of nodes in the Leader's deployment.
   *    We subscribe AFTER adding these entries just to avoid receiving events for inserting the new ACK entries, as
   *    we'd waste time processing those events (albeit a small amount of time).
   *
   * @param writeStartTime The UTC timestamp at which this write operation began.
   *
   * @return The number of ACK records that we added to intermediate storage.
   */
  private List<WriteAcknowledgement> addAckTableRecords(ServerlessNameNode serverlessNameNode, long writeStartTime)
          throws Exception {
    requestHandlerLOG.debug("=-----=-----= Step 2 - Adding ACK Records =-----=-----=");

    ZKClient zkClient = serverlessNameNode.getZooKeeperClient();
    List<String> groupMemberIds = zkClient.getGroupMembers(serverlessNameNode.getFunctionName());
    List<ActiveNode> activeNodes = serverlessNameNode.getActiveNameNodes().getActiveNodes();
    requestHandlerLOG.debug("Active NameNodes at start of consistency protocol: " + activeNodes.toString());

    WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
            (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);

    List<WriteAcknowledgement> writeAcknowledgements = new ArrayList<WriteAcknowledgement>();

    // Iterate over all the current group members. For each group member, we create a WriteAcknowledgement object,
    // which we'll persist to intermediate storage. We skip ourselves, as we do not need to ACK our own write. We also
    // create an entry for each follower NN in the `writeAckMap` to keep track of whether they've ACK'd their entry.
    for (int i = 0; i < groupMemberIds.size(); i++) {
      String memberIdAsString = groupMemberIds.get(i);
      long memberId = Long.parseLong(memberIdAsString);

      // We do not need to add an entry for ourselves.
      if (memberId == serverlessNameNode.getId())
        continue;

      waitingForAcks.add(memberId);
      writeAcknowledgements.add(new WriteAcknowledgement(memberId, serverlessNameNode.getDeploymentNumber(),
              operationId, false, writeStartTime));
    }

    if (writeAcknowledgements.size() > 0) {
      requestHandlerLOG.debug("Preparing to add " + writeAcknowledgements.size()
              + " write acknowledgement(s) to intermediate storage.");
      writeAcknowledgementDataAccess.addWriteAcknowledgements(writeAcknowledgements);
    } else {
      requestHandlerLOG.debug("We're the only Active NN rn. No need to create any ACK entries.");
    }

    // Instantiate the CountDownLatch variable. The value is set to the number of ACKs that we need
    // before we can proceed with the transaction. Receiving an ACK and a follower NN leaving the group
    // will trigger a decrement.
    countDownLatch = new CountDownLatch(writeAcknowledgements.size());

    // This will be zero if we are the only active NameNode.
    return writeAcknowledgements;
  }

  /**
   * Perform Step (1) of the consistency protocol:
   *    The leader sets the INV flag of the target INode to 1 (i.e., true), thereby triggering a round of
   *    INVs from intermediate storage (NDB).
   *
   * @param invalidatedINodes The INodes involved in this write operation. We must invalidate these INodes.
   */
  private void issueInitialInvalidations(
          Collection<INode> invalidatedINodes) throws StorageException {
    requestHandlerLOG.debug("=-----=-----= Step 1 - Issuing Initial Invalidations =-----=-----=");

    INodeDataAccess<INode> dataAccess =
            (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
    long[] ids = invalidatedINodes.stream().mapToLong(INode::getId).toArray();
    dataAccess.setInvalidFlag(ids, true);
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
