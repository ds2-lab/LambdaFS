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

import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.context.INodeContext;
import io.hops.transaction.lock.HdfsTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.BaseHandler;
import org.apache.hadoop.hdfs.serverless.consistency.ConsistencyProtocol;

import java.io.IOException;
import java.util.*;

public abstract class HopsTransactionalRequestHandler
        extends TransactionalRequestHandler {

  private ServerlessNameNode serverlessNameNodeInstance;

  /**
   * When this is set to true, the consistency protocol will be skipped when this transaction request handler
   * attempts to execute it. We use this mechanism during subtree operations, as we execute one instance of the
   * consistency protocol at the beginning of the subtree operation. Then, all the individual transactions that
   * occur during the remainder of the subtree operation do not have to execute the consistency protocol.
   */
  private final boolean skipConsistencyProtocol;

  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null, false);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    this(opType, path, false);
  }

  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path, boolean skipConsistencyProtocol) {
    super(opType);
    this.skipConsistencyProtocol = skipConsistencyProtocol;
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new HdfsTransactionalLockAcquirer();
  }

  @Override
  public void commitEvents() {
    if (ServerlessNameNode.benchmarkingModeEnabled.get()) return;

    // If the local `serverlessNameNodeInstance` variable is null, then we try to grab the static instance
    // from the ServerlessNameNode class directly. It is possible that both will be null, however.
    ServerlessNameNode instance = (serverlessNameNodeInstance == null) ?
            ServerlessNameNode.tryGetNameNodeInstance(false) :
            serverlessNameNodeInstance;

    if (instance == null) {
      ThreadLocal<Set<TransactionEvent>> tempEventsThreadLocal = BaseHandler.temporaryEventSet;
      Set<TransactionEvent> tempEvents = tempEventsThreadLocal.get();

      if (tempEvents == null) {
        tempEvents = new HashSet<>();
        tempEventsThreadLocal.set(tempEvents);
      }

      tempEvents.add(this.transactionEvent);
    } else {
      // Try to get the current request ID from the base handler. If, for whatever reason, the current
      // request ID is null, then just generate and use a random UUID to ensure the hash is unique.
      String currentRequestId = BaseHandler.currentRequestId.get();
      if (currentRequestId == null) currentRequestId = UUID.randomUUID().toString();
      this.transactionEvent.setRequestId(currentRequestId);

      // This adds to a set, so multiple adds won't mess anything up.
      instance.addTransactionEvent(this.transactionEvent);
    }
  }
  
  @Override
  protected Object execute(final Object namesystem) throws IOException {
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
    if (!ConsistencyProtocol.DO_CONSISTENCY_PROTOCOL) {
      requestHandlerLOG.debug("Consistency protocol is DISABLED. Not executing consistency protocol.");
      return true;
    }

    if (skipConsistencyProtocol) {
      requestHandlerLOG.debug("Skipping consistency protocol (presumably because we're within a subtree op).");
      return true;
    }

    INodeContext inodeContext= (INodeContext)EntityManager.getEntityContext(INode.class);
    serverlessNameNodeInstance = ServerlessNameNode.tryGetNameNodeInstance(false);

    // We can check if we need to run the consistency protocol early. We already have access to the INodeContext,
    // and thus we have access to the collection of invalidated INodes. If the size of this collection is zero, then
    // we can skip the consistency protocol altogether. If it is non-zero, then we can pass the set to the consistency
    // protocol object so that it doesn't have to be created again during the actual execution of the protocol.
    boolean shouldRunConsistencyProtocol = true;
    Collection<INode> invalidatedINodes = null;
    if (inodeContext != null) {
      invalidatedINodes = inodeContext.getInvalidatedINodes();
      int numInvalidated = invalidatedINodes.size();
      shouldRunConsistencyProtocol = (numInvalidated > 0);
    }

    // If we should run the protocol (i.e., if the size of the collection of invalidated INodes is greater than 0),
    // then we will run it. Otherwise, we skip it altogether.
    if (shouldRunConsistencyProtocol) {
      ConsistencyProtocol consistencyProtocol = new ConsistencyProtocol(inodeContext, null,
              attempt, transactionEvent, txStartTime,
              // If NN instance is currently null, then we default to using ZooKeeper. If the NN instance is non-null,
              // then we use the configured value.
              serverlessNameNodeInstance == null || !serverlessNameNodeInstance.useNdbForConsistencyProtocol(),
              false, null, invalidatedINodes);

      // Pre-compute the ACKs.
      // This will allow us to avoid starting the thread if it turns out that no ACKs will be required.
      // So, we may be able to bypass the thread creation overhead, but not the object-creation overhead.
      int totalAcksRequired = consistencyProtocol.precomputeAcks();
      if (totalAcksRequired == 0)
        return true;

      // Check if we should bother running. This potentially saves us from starting another thread and joining ,
      // it which is needlessly expensive if we aren't going to bother running the consistency protocol anyway.
      consistencyProtocol.start();
      try {
        consistencyProtocol.join();

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
      } catch (InterruptedException ex) {
        throw new IOException("Encountered InterruptedException while waiting for consistency protocol to finish: ", ex);
      }
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
