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

    EntityContext<?> inodeContext= EntityManager.getEntityContext(INode.class);

    serverlessNameNodeInstance = ServerlessNameNode.tryGetNameNodeInstance(false);

    ConsistencyProtocol consistencyProtocol = new ConsistencyProtocol(inodeContext, null,
            attempt, transactionEvent, txStartTime,
            // If NN instance is currently null, then we default to using ZooKeeper. If the NN instance is non-null,
            // then we use the configured value.
            serverlessNameNodeInstance == null || !serverlessNameNodeInstance.useNdbForConsistencyProtocol(),
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
