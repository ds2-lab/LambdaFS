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
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionInfo;
import io.hops.transaction.context.TransactionsStats;
import io.hops.transaction.lock.Lock;
import io.hops.transaction.lock.TransactionLockAcquirer;
import io.hops.transaction.lock.TransactionLocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class TransactionalRequestHandler extends RequestHandler {

  public TransactionalRequestHandler(OperationType opType) {
    super(opType);
  }

  @Override
  protected Object execute(Object info) throws IOException {
    boolean committed = false;
    int tryCount = 0;
    IOException ignoredException;
    Object txRetValue = null;
    List<Throwable> exceptions = new ArrayList<>();

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


    while (tryCount <= RETRY_COUNT) {
      long expWaitTime = exponentialBackoff();
      long txStartTime = System.currentTimeMillis();
      long oldTime = System.currentTimeMillis();

      long setupTime = -1;
      long beginTxTime = -1;
      long acquireLockTime = -1;
      long inMemoryProcessingTime = -1;
      long commitTime = -1;
      long totalTime = -1;
      TransactionLockAcquirer locksAcquirer = null;

      tryCount++;
      ignoredException = null;
      committed = false;

      requestHandlerLOG.debug("Setting preventStorageCalls to FALSE.");
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
        setupTime = (System.currentTimeMillis() - oldTime);
        oldTime = System.currentTimeMillis();
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("Pre-transaction phase finished. Time " + setupTime + " ms");
        }
        setRandomPartitioningKey();
        EntityManager.begin();
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.trace("TX Started");
        }
        beginTxTime = (System.currentTimeMillis() - oldTime);
        oldTime = System.currentTimeMillis();
        
        locksAcquirer = newLockAcquirer();
        acquireLock(locksAcquirer.getLocks());

        locksAcquirer.acquire();

        acquireLockTime = (System.currentTimeMillis() - oldTime);
        oldTime = System.currentTimeMillis();
        if(requestHandlerLOG.isTraceEnabled()){
          requestHandlerLOG.debug("All Locks Acquired. Time " + acquireLockTime + " ms");
        }
        //sometimes in setup we call light weight request handler that messes up with the NDC
        removeNDC();
        setNDC(info);
        requestHandlerLOG.debug("Setting preventStorageCalls to TRUE.");
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
        inMemoryProcessingTime = (System.currentTimeMillis() - oldTime);
        oldTime = System.currentTimeMillis();
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.debug("In Memory Processing Finished. Time " + inMemoryProcessingTime + " ms");
        }

        TransactionsStats.TransactionStat stat = TransactionsStats.getInstance()
            .collectStats(opType,
            ignoredException);

        EntityManager.commit(locksAcquirer.getLocks());
        committed = true;
        commitTime = (System.currentTimeMillis() - oldTime);
        if(stat != null){
          stat.setTimes(acquireLockTime, inMemoryProcessingTime, commitTime);
        }

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
                  "CommitTime: " + commitTime + "ms. Locks: "+ getINodeLockInfo(locksAcquirer.getLocks()));
        }
        //post TX phase
        //any error in this phase will not re-start the tx
        //TODO: XXX handle failures in post tx phase
        if (info != null && info instanceof TransactionInfo) {
          ((TransactionInfo) info).performPostTransactionAction();
        }
      } catch (Throwable t) {
        boolean suppressException = suppressFailureMsg(t, tryCount);
        if (!suppressException ) {
          String opName = !NDCWrapper.NDCEnabled() ? opType + " " : "";
          for(String subOpName: subOpNames){
            opName = opName + ":" + subOpName;
          }
          String inodeLocks = locksAcquirer != null ? getINodeLockInfo(locksAcquirer.getLocks()):"";
          requestHandlerLOG.warn(opName + "TX Failed. " +
                  "TX Time: " + (System.currentTimeMillis() - txStartTime) + " ms, " +
                  // -1 because 'tryCount` also counts the initial attempt which is technically not a retry
                  "RetryCount: " + (tryCount-1) + ", " +
                  "TX Stats -- Setup: " + setupTime + "ms, AcquireLocks: " + acquireLockTime + "ms, " +
                  "InMemoryProcessing: " + inMemoryProcessingTime + "ms, " +
                  "CommitTime: " + commitTime + "ms. Locks: "+inodeLocks+". " + t, t);
        }
        if (!(t instanceof TransientStorageException) ||  tryCount > RETRY_COUNT) {
          for(Throwable oldExceptions : exceptions) {
            requestHandlerLOG.warn("Exception caught during previous retry: "+oldExceptions, oldExceptions);
          }
          throw t;
        } else{
          if(suppressException)
            exceptions.add(t);
        }
      } finally {
        removeNDC();
        if (!committed && locksAcquirer!=null) {
          try {
            requestHandlerLOG.debug("TX Failed. Rollback TX");
            EntityManager.rollback(locksAcquirer.getLocks());
          } catch (Exception e) {
            requestHandlerLOG.warn("Could not rollback transaction", e);
          }
        }
        //make sure that the context has been removed
        EntityManager.removeContext(); 
      }
      if(success){
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
