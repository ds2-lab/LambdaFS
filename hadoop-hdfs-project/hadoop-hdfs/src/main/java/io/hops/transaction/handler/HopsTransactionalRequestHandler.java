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

import io.hops.leader_election.node.ActiveNode;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.lock.HdfsTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.io.IOException;
import java.util.List;

public abstract class HopsTransactionalRequestHandler
    extends TransactionalRequestHandler {
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType, path);
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new HdfsTransactionalLockAcquirer();
  }

  
  @Override
  protected Object execute(final Object namesystem) throws IOException {

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
    // (3) The leader sets the INV flag of the target INode to 1 (i.e., true), thereby triggering a round of INVs from
    //     intermediate storage (NDB).
    // (4) Each follower NN will ACK its entry in the ACKs table upon receiving the INV from intermediate storage (NDB).
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

    if (opType.shouldUseConsistencyProtocol()) {
      requestHandlerLOG.debug("Transaction type " + opType.getName()
              + " SHOULD use the serverless consistency protocol.");
    } else {
      requestHandlerLOG.debug("Transaction type " + opType.getName()
              + " does NOT need to use the serverless consistency protocol.");
    }
    requestHandlerLOG.debug("Transaction is operating on path: " + path);

    if (namesystem instanceof FSNamesystem) {
      FSNamesystem namesystemInst = (FSNamesystem)namesystem;
      List<ActiveNode> activeNodes = namesystemInst.getActiveNameNodesInDeployment();
      requestHandlerLOG.debug("Active nodes: " + activeNodes.toString());
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
