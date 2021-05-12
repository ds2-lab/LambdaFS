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
package io.hops.transaction.lock;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;

import java.io.IOException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Lock implements Comparable<Lock> {

  protected final static Log LOG = LogFactory.getLog(Lock.class);
  /*
   * The Order of entries in Type defines the order
   * by which it's acquired
   */
  public static enum Type {
    INode,
    NameNodeLease,
    Lease,
    LeasePath,
    AllCachedBlock,
    Block,
    Variable,
    LeDescriptor,
    Replica,
    CorruptReplica,
    ExcessReplica,
    ReplicaUnderConstruction,
    InvalidatedBlock,
    UnderReplicatedBlock,
    PendingBlock,
    QuotaUpdate,
    EncodingStatus,
    BlockChecksum,
    SubTreePath, 
    Test,
    HashBucket,
    Ace,
    retryCachEntry,
    cacheDirective,
    cachePool,
    CachedBlock,
    XAttr,
    EZ
  }

  protected Lock() {

  }

  protected final static TransactionLockTypes.LockType DEFAULT_LOCK_TYPE =
      TransactionLockTypes.LockType.READ_COMMITTED;
  
  protected abstract void acquire(TransactionLocks locks) throws IOException;
  
  protected abstract Type getType();

  @Override
  public int compareTo(Lock o) {
    return getType().compareTo(o.getType());
  }
  
  protected static void setLockMode(TransactionLockTypes.LockType mode)
      throws StorageException {
    switch (mode) {
      case WRITE:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }

  protected <T> Collection<T> acquireLockList(
      TransactionLockTypes.LockType lock, FinderType<T> finder, Object... param)
      throws StorageException, TransactionContextException {

    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  protected <T> T acquireLock(TransactionLockTypes.LockType lock,
      FinderType<T> finder, Object... param)
      throws StorageException, TransactionContextException {

    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }
}
