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

import io.hops.exception.*;
import io.hops.log.NDCWrapper;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.TransactionsStats;
import io.hops.transaction.lock.Lock;
import io.hops.transaction.lock.TransactionLockAcquirer;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public abstract class TransactionalRequestHandler extends RequestHandler {
  private static Random rng = new Random();

  public TransactionalRequestHandler(OperationType opType) {
    super(opType);
    this.operationId = rng.nextLong();
  }

  /**
   * Used as a unique identifier for the operation. This is only used during write operations.
   */
  protected final long operationId;

  /**
   * Used for event-style data collection of transaction timings.
   */
  protected TransactionEvent transactionEvent;

  protected boolean printSuccessMessage = false;

  /**
   * If False, we do not bother with creating TX events.
   */
  public static boolean TX_EVENTS_ENABLED = true;

  /**
   * Override this function to implement a serverless consistency protocol. This will get called
   * AFTER local, in-memory processing occurs but (immediately) before the changes are committed
   * to NDB.
   *
   * @param txStartTime The time at which the transaction began. Used to order operations.
   * @param attempt TransactionAttempt object, used to record metrics about the consistency protocol.
   *
   * @return True if the transaction can safely proceed, otherwise false.
   */
  protected abstract boolean consistencyProtocol(long txStartTime, TransactionAttempt attempt) throws IOException;

  /**
   * Should be overridden by a class in the main codebase. This function should be used
   * to save the {@link TransactionEvent} (and the contained {@link TransactionAttempt} instances
   * within) in order for them to be analyzed/plotted.
   */
  public abstract void commitEvents();

  @Override
  protected Object execute(Object info) throws IOException {
    boolean committed = false;
    int tryCount = 0;
    IOException ignoredException;
    Object txRetValue = null;
    List<Throwable> exceptions = new ArrayList<>();

    if (TX_EVENTS_ENABLED) {
      // Record timings of the various stages in the event.
      transactionEvent = new TransactionEvent(operationId);
      transactionEvent.setTransactionStartTime(System.currentTimeMillis());
    }

    TransactionAttempt transactionAttempt = null;
    while (tryCount <= RETRY_COUNT) {
      if (TX_EVENTS_ENABLED) {
        transactionAttempt = new TransactionAttempt(tryCount);
        transactionEvent.addAttempt(transactionAttempt);
      }

      long expWaitTime = exponentialBackoff();
      long txStartTime = System.currentTimeMillis();
      long oldTime = System.currentTimeMillis();

      long setupTime = -1;
      long beginTxTime = -1;
      long acquireLockTime = -1;
      long inMemoryProcessingTime = -1;
      long consistencyProtocolTime = -1;
      long commitTime = -1;
      long totalTime = -1;
      TransactionLockAcquirer locksAcquirer = null;

      tryCount++;
      ignoredException = null;
      committed = false;

      EntityManager.preventStorageCall(false);
      boolean success = false;
      try {
        setNDC(info);
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("Pre-transaction phase started");
        }
        preTransactionSetup();
        //sometimes in setup we call light weight request handler that messes up with the NDC
        removeNDC();
        setNDC(info);
        long lockAcquireStartTime = System.currentTimeMillis();
        setupTime = (lockAcquireStartTime - oldTime);

        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("Pre-transaction phase finished. Time " + setupTime + " ms");
        }
        setRandomPartitioningKey();
        EntityManager.begin();
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.trace("TX Started");
        }
        //beginTxTime = (System.currentTimeMillis() - oldTime);
        oldTime = System.currentTimeMillis();
        
        locksAcquirer = newLockAcquirer();
        // Basically populate the TransactionLocks object with the locks that we need to acquire.
        // This doesn't actually do any acquiring of locks as far as I know.
        acquireLock(locksAcquirer.getLocks());

        // This actually acquires the locks and reads the metadata into memory from intermediate storage.
        locksAcquirer.acquire();

        long inMemoryStart = System.currentTimeMillis();
        acquireLockTime = (inMemoryStart - oldTime);
        if(requestHandlerLOG.isTraceEnabled()){
          requestHandlerLOG.debug("All Locks Acquired. Time " + acquireLockTime + " ms");
        }
        //sometimes in setup we call lightweight request handler that messes up with the NDC
        removeNDC();
        setNDC(info);
        EntityManager.preventStorageCall(true);
        try {
          txRetValue = performTask();
          success = true;
        } catch (IOException e) {
          if (shouldAbort(e)) {
            throw e;
          } else {
            ignoredException = e;
          }
        }
        long consistencyStartTime = System.currentTimeMillis();
        inMemoryProcessingTime = (consistencyStartTime - inMemoryStart);

        oldTime = System.currentTimeMillis();
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("In Memory Processing Finished. Time " + inMemoryProcessingTime + " ms");
        }

        TransactionsStats.TransactionStat stat = TransactionsStats.getInstance()
            .collectStats(opType,
            ignoredException);

        boolean canProceed = consistencyProtocol(txStartTime, transactionAttempt);

        long commitStart = System.currentTimeMillis();
        consistencyProtocolTime = (commitStart - oldTime);
        oldTime = System.currentTimeMillis();

        if (canProceed) {
          if (printSuccessMessage)
            requestHandlerLOG.debug("Consistency protocol for TX " + operationId + " succeeded after " + consistencyProtocolTime + " ms");
        } else {
          if (TX_EVENTS_ENABLED && transactionAttempt != null) {
            transactionAttempt.setCommitEnd(System.currentTimeMillis());
            transactionAttempt.setAcquireLocksStart(lockAcquireStartTime);
            transactionAttempt.setAcquireLocksEnd(inMemoryStart);
            transactionAttempt.setProcessingStart(inMemoryStart);
            transactionAttempt.setProcessingEnd(consistencyStartTime);
            transactionAttempt.setConsistencyProtocolStart(consistencyStartTime);
            transactionAttempt.setConsistencyProtocolEnd(commitStart);
            transactionAttempt.setConsistencyProtocolSucceeded(false);
            transactionAttempt.setCommitStart(commitStart);
          }
          throw new IOException("Consistency protocol for TX " + operationId + " FAILED after " +
                  consistencyProtocolTime + " ms.");
        }

        TransactionLocks transactionLocks = locksAcquirer.getLocks();

        // We have now acquired the locks. At this point, we should check for any new write operations on
        // the INodes we're updating. Since we hold locks, no new operations can come along and change the `INV` flag,
        // so whatever exists now is what is going to exist until we finish the transaction.
        //
        // If there is a new write operation that has occurred after ours, we do not want to flip the `INV` flags back
        // to false, as that will screw up the next write operation. So, our goal is to check for the existence of
        // a later write and, if one exists, make sure all of our nodes have their `INV` flags set to true.
        // checkAndHandleNewConcurrentWrites(txStartTime);

        EntityManager.commit(transactionLocks);
        committed = true;
        commitTime = (System.currentTimeMillis() - oldTime);

        if (TX_EVENTS_ENABLED && transactionAttempt != null) {
          transactionAttempt.setCommitEnd(System.currentTimeMillis());
          transactionAttempt.setAcquireLocksStart(lockAcquireStartTime);
          transactionAttempt.setAcquireLocksEnd(inMemoryStart);
          transactionAttempt.setProcessingStart(inMemoryStart);
          transactionAttempt.setProcessingEnd(consistencyStartTime);
          transactionAttempt.setConsistencyProtocolStart(consistencyStartTime);
          transactionAttempt.setConsistencyProtocolEnd(commitStart);
          transactionAttempt.setConsistencyProtocolSucceeded(canProceed);
          transactionAttempt.setCommitStart(commitStart);
        }

        if(stat != null)
          stat.setTimes(acquireLockTime, inMemoryProcessingTime, commitTime);

        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("TX committed. Time " + commitTime + " ms");
        }
        totalTime = (System.currentTimeMillis() - txStartTime);
        if(requestHandlerLOG.isTraceEnabled()) {
          String opName = !NDCWrapper.NDCEnabled()?opType+" ":"";
          requestHandlerLOG.debug(opName+"TX Finished. " +
                  "TX Time: " + (totalTime) + " ms, " +
                  // -1 because 'tryCount` also counts the initial attempt which is technically not a retry
                  "RetryCount: " + (tryCount-1) + ", " +
                  "TX Stats -- Setup: " + setupTime + "ms, AcquireLocks: " + acquireLockTime + "ms, " +
                  "InMemoryProcessing: " + inMemoryProcessingTime + "ms, " +
                  "ConsistencyProtocol: " + consistencyProtocolTime + "ms, " +
                  "CommitTime: " + commitTime + "ms. Locks: "+ getINodeLockInfo(locksAcquirer.getLocks()));
        }
        //post TX phase
        //any error in this phase will not re-start the tx
        //TODO: XXX handle failures in post tx phase
        if (info != null && info instanceof TransactionInfo) {
          ((TransactionInfo) info).performPostTransactionAction();
        }
      }
      catch (Throwable t) {
        // If the commit start time is set, then we've entered the commit phase.
        // Since we encountered an exception, we should end the commit phase here.
        if (TX_EVENTS_ENABLED && transactionAttempt != null && transactionAttempt.getCommitStart() > 0)
          transactionAttempt.setCommitEnd(System.currentTimeMillis());

        boolean suppressException = suppressFailureMsg(t, tryCount);
        if (!suppressException ) {
          String opName = !NDCWrapper.NDCEnabled() ? opType + " " : "";
          for(String subOpName: subOpNames){
            opName = opName + ":" + subOpName;
          }
          String inodeLocks = locksAcquirer != null ? getINodeLockInfo(locksAcquirer.getLocks()):"";
          requestHandlerLOG.warn(opName + "TX " + operationId + " Failed. " +
                  "TX Time: " + (System.currentTimeMillis() - txStartTime) + " ms, " +
                  // -1 because 'tryCount` also counts the initial attempt which is technically not a retry
                  "RetryCount: " + (tryCount-1) + ", " +
                  "TX Stats -- Setup: " + setupTime + "ms, AcquireLocks: " + acquireLockTime + "ms, " +
                  "InMemoryProcessing: " + inMemoryProcessingTime + "ms, " +
                  "CommitTime: " + commitTime + "ms. Locks: "+inodeLocks+". " + t, t);
        }
        if (!(t instanceof TransientStorageException) ||  tryCount > RETRY_COUNT) {
          commitEvents();
          for(Throwable oldExceptions : exceptions) {
            requestHandlerLOG.warn("Exception caught during previous retry: "+oldExceptions, oldExceptions);
          }
          throw t;
        } else{
          if(suppressException)
            exceptions.add(t);
        }
      }
      finally {
        removeNDC();
        if (!committed && locksAcquirer!=null) {
          try {
            requestHandlerLOG.warn("TX " + operationId + " Failed. Rolling back now...");
            // requestHandlerLOG.debug(Arrays.toString((new Throwable()).getStackTrace()));
            EntityManager.rollback(locksAcquirer.getLocks());
          } catch (Exception e) {
            requestHandlerLOG.warn("Could not rollback transaction", e);
          }
        }
        //make sure that the context has been removed
        EntityManager.removeContext();
        commitEvents();
      }

      transactionEvent.setTransactionEndTime(System.currentTimeMillis());
      transactionEvent.setSuccess(success);

      commitEvents();

      if(success) {
        // If the code is about to return but the exception was caught
        if (ignoredException != null) {
          throw ignoredException;
        }
        return txRetValue;
      }
    }
    throw new RuntimeException("TransactionalRequestHandler did not execute");
  }

  private boolean suppressFailureMsg(Throwable t, int tryCount ){
    boolean suppressFailureMsg = false;
    if (opType.toString().equals("GET_BLOCK_LOCATIONS") && t instanceof LockUpgradeException ) {
      suppressFailureMsg = true;
    } else if (opType.toString().equals("COMPLETE_FILE") && t instanceof OutOfDBExtentsException ) {
      suppressFailureMsg = true;
    } else if ( t instanceof TransientDeadLockException ){
      suppressFailureMsg = true;
    }

    if( suppressFailureMsg && tryCount <= RETRY_COUNT ){
      return true;
    }else {
      return false;
    }
  }

  private String getINodeLockInfo(TransactionLocks locks){
    String inodeLockMsg = "";
    try {
      if(locks != null) {
        Lock ilock = locks.getLock(Lock.Type.INode);
        if (ilock != null) {
        inodeLockMsg = ilock.toString();
        }
      }
    }catch (TransactionLocks.LockNotAddedException e){
    }
    return inodeLockMsg;
  }

  protected abstract void preTransactionSetup() throws IOException;
  
  public abstract void acquireLock(TransactionLocks locks) throws IOException;
  
  protected abstract TransactionLockAcquirer newLockAcquirer();

//  /**
//   * This should be called after locks are acquired for the transaction. This function checks for pending
//   * ACKs in the `write_acknowledgements` table whose associated TX times are >= the current TX being performed
//   * locally. Specifically, this checks for pending ACKs whose target NN ID is the local NN's ID (i.e., the local
//   * ServerlessNameNode instance running in this container) and whose TX times satisfy the aforementioned constraint.
//   *
//   * If there are any such pending ACKs, we do NOT acknowledge them yet. Instead, for each ACK entry whose target NN ID
//   * is that of our local NN instance and whose write time is >= the local TX start time, we ensure the associated
//   * INode has `INV` set to true.
//   *
//   * @param txStartTime The time at which this transaction began.
//   */
//  protected abstract void checkAndHandleNewConcurrentWrites(long txStartTime) throws StorageException;

  @Override
  public TransactionalRequestHandler setParams(Object... params) {
    this.params = params;
    return this;
  }

  private void setNDC(Object info) {
    // Defines a context for every operation to track them in the logs easily.
    if (info != null && info instanceof TransactionInfo) {
      NDCWrapper.push(((TransactionInfo) info).getContextName(opType));
    } else {
      NDCWrapper.push(opType.toString());
    }
  }
  
  private void removeNDC() {
    NDCWrapper.clear();
    NDCWrapper.remove();
  }

  private void setRandomPartitioningKey()
      throws StorageException, StorageException {
    //      Random rand =new Random(System.currentTimeMillis());
    //      Integer partKey = new Integer(rand.nextInt());
    //      //set partitioning key
    //      Object[] pk = new Object[2];
    //      pk[0] = partKey;
    //      pk[1] = Integer.toString(partKey);
    //
    //      EntityManager.setPartitionKey(INodeDataAccess.class, pk);
    //
    ////      EntityManager.readCommited();
    ////      EntityManager.find(INode.Finder.ByPK_NameAndParentId, "", partKey);
    
  }

  protected abstract boolean shouldAbort(Exception e);
  
  protected void addSubopName(String name){
    subOpNames.add(name);
  }
}
