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
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;

public abstract class EntityContext<T> {

  protected static String NOT_SUPPORTED_YET = "Not supported yet.";
  protected static String UNSUPPORTED_FINDER = "Unsupported finder.";
  protected static String UNSUPPORTED_COUNTER = "Unsupported counter.";
  private static final Log LOG = LogFactory.getLog(TransactionContext.class);
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";
  protected boolean storageCallPrevented = false;

  public enum LockMode {
    READ_LOCK,
    WRITE_LOCK,
    READ_COMMITTED
  }

  final protected static ThreadLocal<LockMode> currentLockMode =
      new ThreadLocal<>();

  /**
   * We set this to false when performing a write operation and true when performing a read operation.
   */
  final protected static ThreadLocal<Boolean> metadataCacheReadsEnabled = ThreadLocal.withInitial(() -> true);

  /**
   * Used to prevent writes/updates to the local metadata cache. We toggle this in the BlockManager, as it
   * often reads batches of INodes (without their parents) from NDB, and caching an INode requires the full
   * path, so trying to resolve huge batches of INodes' parents from NDB can cause problems.
   */
  final protected static ThreadLocal<Boolean> metadataCacheWritesEnabled = ThreadLocal.withInitial(() -> true);

  /**
   * Defines the cache state of the request. This enum is only used for logging
   * purpose.
   */
  enum CacheHitState {
    HIT,
    LOSS,
    LOSS_LOCK_UPGRADE,
    NA
  }

  /**
   * @throws io.hops.exception.TransactionContextException
   *     If the context encounters an illegal state
   * @throws io.hops.exception.StorageException
   *     If database errors occur
   */
  public abstract void add(T entity)
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   */
  public abstract void clear() throws TransactionContextException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   * @throws StorageException
   *     If database errors occur
   */
  public abstract int count(CounterType<T> counter, Object... params)
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   * @throws StorageException
   *     If database errors occur
   */
  public abstract T find(FinderType<T> finder, Object... params)
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   * @throws StorageException
   *     If database errors occur
   */
  public abstract Collection<T> findList(FinderType<T> finder, Object... params)
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   * @throws StorageException
   *     If database errors occur
   */
  public abstract void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   */
  public abstract void remove(T entity) throws TransactionContextException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   * @throws StorageException
   *     If database errors occur
   */
  public abstract void removeAll()
      throws TransactionContextException, StorageException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   */
  public abstract void update(T entity) throws TransactionContextException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   */
  public abstract void snapshotMaintenance(
      TransactionContextMaintenanceCmds cmds, Object... params)
      throws TransactionContextException;

  /**
   * @throws TransactionContextException
   *     If the context encounters an illegal state
   */
  public EntityContextStat collectSnapshotStat()
      throws TransactionContextException {
    throw new UnsupportedOperationException("Please Implement Me");
  }

  protected boolean isLogTraceEnabled(){
    return LOG.isTraceEnabled();
  }

  private static void log(String opName, CacheHitState state,
      Object... params) {
    if (!LOG.isTraceEnabled()) return;
    StringBuilder message = new StringBuilder();
    if (state == CacheHitState.HIT) {
      message.append(ANSI_GREEN).append(opName).append(" ").append("hit")
          .append(ANSI_RESET);
    } else if (state == CacheHitState.LOSS) {
      LockMode curLock = currentLockMode.get();
      message.append(ANSI_RED);
      if (curLock != null) {
        message.append(curLock.name()).append(" ");
      }
      message.append(opName).append(" ").append("loss").append(ANSI_RESET);
    } else if (state == CacheHitState.LOSS_LOCK_UPGRADE) {
      LockMode curLock = currentLockMode.get();
      message.append(ANSI_BLUE);
      if (curLock != null) {
        message.append(curLock.name()).append(" ");
      }
      message.append(opName).append(" ").append("loss").append(ANSI_RESET);
    } else {
      message.append(opName).append(" ");
    }
    message.append(" ");
    if (params.length > 1) {
      for (int i = 0; i < params.length; i = i + 2) {
        message.append(" ").append(params[i]);
        message.append("=").append(params[i + 1]);
      }
    }
    LOG.trace(message.toString());
  }

  protected void logError(String msg) {
    StringBuilder message = new StringBuilder();
    message.append(ANSI_RED);
    message.append(msg).append(" ");
    message.append(ANSI_RESET);
    LOG.fatal(message.toString());
  }

  protected void log(String opName, Object... params) {
    log(opName, CacheHitState.NA, params);
  }

  public static void log(FinderType finderType, CacheHitState state,
      Object... params) {
    if (!LOG.isTraceEnabled()) return;
    log(getOperationMessage(finderType), state, params);
  }

  private static String getOperationMessage(FinderType finder) {
    return "find-" + finder.getType().getSimpleName().toLowerCase() + "-" + finder;
  }

  public void preventStorageCall(boolean val) {
    storageCallPrevented = val;
  }

  public static void setLockMode(LockMode lock) {
    currentLockMode.set(lock);
  }

  public static LockMode getLockMode() {
    return currentLockMode.get();
  }

  /**
   * Returns true if both localMetadataCachedEnabled is true and the current lock mode is not WRITE LOCK.
   */
  public static boolean areMetadataCacheReadsEnabled() {
    return metadataCacheReadsEnabled.get() && currentLockMode.get() != LockMode.WRITE_LOCK;
  }

  public static boolean areMetadataCacheWritesEnabled() {
    return metadataCacheWritesEnabled.get();
  }

  /**
   * Pass true to enable; pass false to disable.
   */
  public static void toggleMetadataCacheReads(boolean enabled) {
    metadataCacheReadsEnabled.set(enabled);
  }

  public static void toggleMetadataCacheWrites(boolean enabled) {
    metadataCacheWritesEnabled.set(enabled);
  }

  protected void aboutToAccessStorage(FinderType finderType)
      throws StorageCallPreventedException {
    aboutToAccessStorage(finderType, "");
  }

  protected void aboutToAccessStorage(FinderType finderType, Object... params)
      throws StorageCallPreventedException {
    if (storageCallPrevented) {
      throw new StorageCallPreventedException("[" + finderType + "] Trying " +
          "to access storage while it is disabled in transaction; inconsistent" +
          " transaction context statement. Params=" + Arrays.toString(params));
    }
  }

  protected boolean preventStorageCalls() {
    return storageCallPrevented;
  }
}
