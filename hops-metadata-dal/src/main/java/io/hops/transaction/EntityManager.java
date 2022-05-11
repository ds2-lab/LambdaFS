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
package io.hops.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.context.ContextInitializer;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.EntityContextStat;
import io.hops.transaction.context.TransactionContext;
import io.hops.transaction.context.TransactionContextMaintenanceCmds;
import io.hops.transaction.handler.RequestHandler;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.hops.transaction.context.EntityContext.*;

public class EntityManager {

  private EntityManager() {
  }
  public static final Logger LOG = LoggerFactory.getLogger(EntityManager.class);

  private static ThreadLocal<TransactionContext> threadContext = new ThreadLocal();
  private static CopyOnWriteArrayList<ContextInitializer> contextInitializers =
      new CopyOnWriteArrayList<>();
  private static boolean initialized = false;

  public static void addContextInitializer(ContextInitializer ci) {
    contextInitializers.add(ci);
    if (!initialized) {
      initialized = true;
      RequestHandler.setStorageConnector(ci.getConnector());
    }
  }

  /**
   * Return the given {@link EntityContext} mapped by the given class, should one exist.
   * Otherwise, returns null.
   */
  public static EntityContext<?> getEntityContext(Class<?> clazz) {
    TransactionContext context = context();

    return context.getEntityContext(clazz);
  }
  
  private static TransactionContext context() {
    TransactionContext context = threadContext.get();
    if (context == null) {
      context = addContext();
    }
    return context;
  }

  public static void begin() throws StorageException {
    context().begin();
  }

  public static void preventStorageCall(boolean val) {
    context().preventStorageCall(val);
  }

  public static void commit(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    context().commit(tlm);
    removeContext();
  }

  public static void rollback(TransactionLocks tlm)
      throws StorageException, TransactionContextException {
    context().rollback();
    removeContext();
  }

  public static <T> void remove(T obj)
      throws StorageException, TransactionContextException {
    context().remove(obj);
  }

  public static void removeAll(Class type)
      throws StorageException, TransactionContextException {
    context().removeAll(type);
  }

  public static <T> Collection<T> findList(FinderType<T> finder,
      Object... params) throws TransactionContextException, StorageException {
    return context().findList(finder, params);
  }

  public static <T> T find(FinderType<T> finder, Object... params)
      throws TransactionContextException, StorageException {
    return context().find(finder, params);
  }

  public static int count(CounterType counter, Object... params)
      throws TransactionContextException, StorageException {
    return context().count(counter, params);
  }

  public static <T> void update(T entity)
      throws TransactionContextException, StorageException {
    context().update(entity);
  }

  public static <T> void add(T entity)
      throws TransactionContextException, StorageException {
    context().add(entity);
  }
  
  public static <T> void snapshotMaintenance(
      TransactionContextMaintenanceCmds cmds, Object... params)
      throws TransactionContextException {
    context().snapshotMaintenance(cmds, params);
  }

  /**
   * Pass true to enable; pass false to disable.
   */
  public static void toggleMetadataCacheReads(boolean enabled) {
    if (enabled) LOG.trace(ANSI_GREEN + "[ENABLING METADATA CACHE READS]" + ANSI_RESET);
    else LOG.trace(ANSI_RED + "[DISABLING METADATA CACHE READS]" + ANSI_RESET);
    EntityContext.toggleMetadataCacheReads(enabled);
  }


  /**
   * Pass true to enable; pass false to disable.
   */
  public static void toggleMetadataCacheWrites(boolean enabled) {
    if (enabled) LOG.trace(ANSI_GREEN + "[ENABLING METADATA WRITES]" + ANSI_RESET);
    else LOG.trace(ANSI_RED + "[DISABLING METADATA WRITES]" + ANSI_RESET);
    EntityContext.toggleMetadataCacheWrites(enabled);
  }
  public static void writeLock() throws StorageException {
    LOG.trace(ANSI_CYAN + "[LOCKING: WRITE]" + ANSI_RESET);
    EntityContext.setLockMode(EntityContext.LockMode.WRITE_LOCK);
    contextInitializers.get(0).getConnector().writeLock();
  }

  public static void readLock() throws StorageException {
    LOG.trace(ANSI_BLUE + "[LOCKING: READ]" + ANSI_RESET);
    EntityContext.setLockMode(EntityContext.LockMode.READ_LOCK);
    contextInitializers.get(0).getConnector().readLock();
  }

  public static void readCommited() throws StorageException {
    LOG.trace(ANSI_PURPLE + "[LOCKING: READ COMMITTED]" + ANSI_RESET);
    EntityContext.setLockMode(EntityContext.LockMode.READ_COMMITTED);
    contextInitializers.get(0).getConnector().readCommitted();
  }

  public static void setPartitionKey(Class name, Object key)
      throws StorageException {
    contextInitializers.get(0).getConnector().setPartitionKey(name, key);
  }
  
  public static Collection<EntityContextStat> collectSnapshotStat()
      throws TransactionContextException {
    return context().collectSnapshotStat();
  }
    
  private static TransactionContext addContext() {
    Map<Class, EntityContext> storageMap = new HashMap<>();
    for (ContextInitializer initializer : contextInitializers) {
      Map<Class, EntityContext> tmp = initializer.createEntityContexts();
      for (Class clzz : tmp.keySet()) {
        storageMap.put(clzz, tmp.get(clzz));
      }
    }
    TransactionContext context =
        new TransactionContext(contextInitializers.get(0).getConnector(),
            storageMap);
    threadContext.set(context);
    return context;
  }

  public static boolean getStorageCallsPreventedINode() {
    if (threadContext.get() == null)
      return false;

    return context().getStorageCallsPreventedINode();
  }
  
  public static void removeContext() {
    threadContext.remove();
  }
}
