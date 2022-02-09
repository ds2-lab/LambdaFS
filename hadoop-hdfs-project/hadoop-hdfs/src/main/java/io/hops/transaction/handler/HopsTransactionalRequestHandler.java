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
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class HopsTransactionalRequestHandler
        extends TransactionalRequestHandler implements HopsEventListener {

  /**
   * Basically just exists to make debugging/performance testing easier. I use this to dynamically
   * enable or disable the consistency protocol, which is used during write transactions.
   */
  public static boolean DO_CONSISTENCY_PROTOCOL = true;

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

    this.serverlessNameNodeInstance = ServerlessNameNode.tryGetNameNodeInstance(true);
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
      // String requestId = instance.getRequestCurrentlyProcessing();
      this.transactionEvent.setRequestId("UNKNOWN");
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
    if (!DO_CONSISTENCY_PROTOCOL) {
      requestHandlerLOG.debug("Skipping consistency protocol as 'DO_CONSISTENCY_PROTOCOL' is set to false.");
      return true;
    }

    EntityContext<?> inodeContext= EntityManager.getEntityContext(INode.class);

    ConsistencyProtocol consistencyProtocol = new ConsistencyProtocol(inodeContext, null,
            attempt, transactionEvent, txStartTime, !serverlessNameNodeInstance.useNdbForConsistencyProtocol(),
            false, null);

    consistencyProtocol.start();
    try {
      consistencyProtocol.join();
    } catch (InterruptedException ex) {
      throw new IOException("Encountered InterruptedException while waiting for consistency protocol to finish: ", ex);
    }

    boolean proceedWithTx = consistencyProtocol.getCanProceed();
    if (!proceedWithTx) {
      requestHandlerLOG.error("Encountered " + consistencyProtocol.getExceptions() +
              " exception(s) while executing the consistency protocol.");
      int counter = 1;
      for (Throwable throwable : consistencyProtocol.getExceptions()) {
        requestHandlerLOG.error("Exception #" + counter++);
        requestHandlerLOG.error(throwable);
      }

      // Just throw the first exception we encountered.
      Throwable t = consistencyProtocol.getExceptions().get(0);

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
