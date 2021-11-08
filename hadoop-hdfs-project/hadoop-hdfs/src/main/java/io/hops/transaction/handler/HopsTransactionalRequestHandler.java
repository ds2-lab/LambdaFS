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
import io.hops.events.EventManager;
import io.hops.events.HopsEvent;
import io.hops.events.HopsEventListener;
import io.hops.events.HopsEventOperation;
import io.hops.exception.StorageException;
import io.hops.leader_election.node.ActiveNode;
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public abstract class HopsTransactionalRequestHandler
        extends TransactionalRequestHandler implements HopsEventListener {

  private final String path;

  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType);
    this.path = path;
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
    // (1) The Leader NN begins listening for changes in group membership from ZooKeeper.
    //     The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly.
    // (2) Add N-1 un-ACK'd records to the "ACKs" table, where N is the number of nodes in the Leader's deployment.
    //     (The Leader adds N-1 as it does not need to add a record for itself.)
    // (3) The leader sets the INV flag of the target INode(s) to 1 (i.e., true), thereby triggering a round of INVs
    //     from intermediate storage (NDB).
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

    requestHandlerLOG.debug("=-=-=-=-= CONSISTENCY PROTOCOL =-=-=-=-=");
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

    // Carry out the consistency protocol.
    subscribeToAckEvents(serverlessNameNode);       // Step 1
    addAckTableRecords(serverlessNameNode);         // Step 2
    issueInitialInvalidations(serverlessNameNode);  // Step 3

    cleanUpAfterConsistencyProtocol();

    return true;
  }

  /**
   * Perform any necessary clean-up steps after the consistency protocol has completed.
   * This includes unsubscribing from ACK table events, removing the ACK entries from the table in NDB, etc.
   *
   * TODO: How does the 'INV' flag get reset? It might be the case that the data we intend to write during the
   *       transaction will have the 'INV' configured to be false (so the data is valid), in which case it happens
   *       automatically.
   */
  private void cleanUpAfterConsistencyProtocol(ServerlessNameNode serverlessNameNode) throws StorageException {
    // Unsubscribe and unregister event listener.
    EventManager eventManager = serverlessNameNode.getNdbEventManager();
    eventManager.removeListener(this, HopsEvent.ACK_TABLE_EVENT_NAME);
    eventManager.unregisterEventOperation(HopsEvent.ACK_TABLE_EVENT_NAME);

    // Remove the ACK entries that we added.
  }

  @Override
  public void eventReceived(HopsEventOperation eventData, String eventName) {
    if (!eventName.equals(HopsEvent.ACK_TABLE_EVENT_NAME))
      requestHandlerLOG.debug("HopsTransactionalRequestHandler received unexpected event " + eventName + "!");
  }

  /**
   * Perform Step (1) of the consistency protocol:
   *    The Leader NN begins listening for changes in group membership from ZooKeeper.
   *    The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly.
   */
  private void subscribeToAckEvents(ServerlessNameNode serverlessNameNode) {
    requestHandlerLOG.debug("=-----=-----= Step 1 - Subscribing to ACK Events =-----=-----=");

    serverlessNameNode.getNdbEventManager().registerEvent()
  }

  /**
   * Perform Step (2) of the consistency protocol:
   *    Add N-1 un-ACK'd records to the "ACKs" table, where N is the number of nodes in the Leader's deployment.
   */
  private void addAckTableRecords(ServerlessNameNode serverlessNameNode) {
    requestHandlerLOG.debug("=-----=-----= Step 2 - Adding ACK Records =-----=-----=");
  }

  /**
   * Perform Step (3) of the consistency protocol:
   *    The leader sets the INV flag of the target INode to 1 (i.e., true), thereby triggering a round of
   *    INVs from intermediate storage (NDB).
   */
  private void issueInitialInvalidations(ServerlessNameNode serverlessNameNode) {
    requestHandlerLOG.debug("=-----=-----= Step 3 - Issuing Initial Invalidations =-----=-----=");
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
