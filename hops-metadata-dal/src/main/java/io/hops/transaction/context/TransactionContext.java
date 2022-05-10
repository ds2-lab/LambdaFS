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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionContext {
  private static String UNKNOWN_TYPE = "Unknown type:";
  private boolean activeTxExpected = false;
  private Map<Class, EntityContext> typeContextMap;
  private Set<EntityContext> contexts = new HashSet<>();
  private StorageConnector connector;

  public TransactionContext(StorageConnector connector,
      Map<Class, EntityContext> entityContext) {
    this.typeContextMap = entityContext;
//    for (EntityContext context : entityContext.values()) {
//      if (!contexts.contains(context)) {
//        contexts.add(context);
//      }
//    }
    contexts.addAll(entityContext.values());
    this.connector = connector;
  }

  /**
   * Return the {@link EntityContext} instance mapped by the given class, should one exist.
   * Otherwise, returns null.
   */
  public EntityContext<?> getEntityContext(Class<?> clazz) {
    return typeContextMap.get(clazz);
  }

  private void resetContext() throws TransactionContextException {
    activeTxExpected = false;
    clearContext();
    EntityContext.setLockMode(null); // null won't be logged
  }

  public void clearContext() throws TransactionContextException {
    for (EntityContext context : contexts) {
      context.clear();
    }
  }

  public void begin() throws StorageException {
    activeTxExpected = true;
    connector.beginTransaction();
  }

  public void preventStorageCall(boolean val) {
    for (EntityContext context : contexts) {
      context.preventStorageCall(val);
    }
  }

  public void commit(final TransactionLocks tlm)
      throws StorageException, TransactionContextException {
    aboutToPerform();
    for (EntityContext context : contexts) {
      context.prepare(tlm);
    }
    connector.commit();
    resetContext();
  }

  public void rollback() throws StorageException, TransactionContextException {
    resetContext();
    connector.rollback();
  }

  public <T> void update(T obj)
      throws StorageException, TransactionContextException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).update(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void add(T obj)
      throws StorageException, TransactionContextException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).add(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void remove(T obj)
      throws StorageException, TransactionContextException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).remove(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public void removeAll(Class type)
      throws StorageException, TransactionContextException {
    aboutToPerform();

    if (typeContextMap.containsKey(type)) {
      typeContextMap.get(type).removeAll();
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + type);
    }
  }

  public <T> T find(FinderType<T> finder, Object... params)
      throws TransactionContextException, StorageException {
    aboutToPerform();
    if (typeContextMap.containsKey(finder.getType())) {
      return (T) typeContextMap.get(finder.getType()).find(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public <T> Collection<T> findList(FinderType<T> finder, Object... params)
      throws TransactionContextException, StorageException {
      aboutToPerform();
    if (typeContextMap.containsKey(finder.getType())) {
      return typeContextMap.get(finder.getType()).findList(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public int count(CounterType counter, Object... params)
      throws StorageException, TransactionContextException {
    aboutToPerform();
    if (typeContextMap.containsKey(counter.getType())) {
      return typeContextMap.get(counter.getType()).count(counter, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + counter.getType());
    }
  }

  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    for (EntityContext context : contexts) {
      context.snapshotMaintenance(cmds, params);
    }
  }
  
  private void aboutToPerform() throws StorageException {
    if (!activeTxExpected) {
      throw new RuntimeException("Transaction is not begun.");
    }
  }
  
  public Collection<EntityContextStat> collectSnapshotStat()
      throws TransactionContextException {
    List<EntityContextStat> stats = new ArrayList<>();
    for (EntityContext context : contexts) {
      EntityContextStat stat = context.collectSnapshotStat();
      if (stat != null) {
        stats.add(stat);
      }
    }
    return stats;
  }

  public boolean getStorageCallsPreventedINode() {
    EntityContext<?> context = typeContextMap.get(INode.class);

    if (context == null) return false;

    return context.storageCallPrevented;
  }
}
