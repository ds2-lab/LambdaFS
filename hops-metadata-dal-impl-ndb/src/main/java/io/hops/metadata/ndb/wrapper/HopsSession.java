/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.*;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.query.QueryBuilder;
import io.hops.events.HopsEventOperation;
import io.hops.exception.StorageException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;

public class HopsSession {
  private final Session session;
  private LockMode lockMode = LockMode.READ_COMMITTED;

  // Using the DescriptiveStatistics objects with transactions and everything would be complicated.
  // The individual calls here may or may not actually perform network I/O depending on if we're
  // in a transaction or not. Could maybe check if currentTransaction() is null or not and, if not
  // then record the statistics, but then I'd have to separately account for the transactions.

//  /**
//   * The read-related statistics across the entire lifetime of this HopsSession object.
//   */
//  private final DescriptiveStatistics lifetimeReadStatistics;
//
//  /**
//   * The write-related statistics across the entire lifetime of this HopsSession object.
//   */
//  private final DescriptiveStatistics lifetimeWriteStatistics;
//
//  /**
//   * The read-related statistics of this HopsSession object for the current operation being performed.
//   */
//  private final DescriptiveStatistics currentOperationReadStatistics;
//
//  /**
//   * The write-related statistics of this HopsSession object for the current operation being performed.
//   */
//  private final DescriptiveStatistics currentOperationWriteStatistics;

  public HopsSession(Session session) {
    this.session = session;

//    this.lifetimeReadStatistics = new DescriptiveStatistics();
//    this.lifetimeWriteStatistics = new DescriptiveStatistics();
//    this.currentOperationReadStatistics = new DescriptiveStatistics();
//    this.currentOperationWriteStatistics = new DescriptiveStatistics();
  }

  static final Log LOG = LogFactory.getLog(HopsSession.class);

  public HopsQueryBuilder getQueryBuilder() throws StorageException {
    try {
      QueryBuilder queryBuilder = session.getQueryBuilder();
      return new HopsQueryBuilder(queryBuilder);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  /**
   * Wait for an event to occur. Will return as soon as an event
   * is available on any of the created events.
   *
   * @param timeMilliseconds The maximum amount of time to wait for an event.
   *                         Specifying -1 will wait for "a long time" (60 seconds, according to the NDB source code).
   *
   * @return True if events available, false if no events available.
   */
  public synchronized boolean pollForEvents(int timeMilliseconds) {
    return session.pollEvents(timeMilliseconds, null);
  }

  /**
   * Drop/unregister the event identified by the given name.
   *
   * @param eventName Unique identifier of the event.
   * @return True if an event was dropped, otherwise False.
   */
  public boolean dropEvent(String eventName) {
    return session.dropEvent(eventName, 0);
  }

  /**
   * Drop the given event operation from the server.
   *
   * @param eventOp The event operation to be dropped.
   * @param eventName The name of the event for which the subscription is being dropped.
   * @return True if the operation was dropped, otherwise false.
   */
  public boolean dropEventOperation(HopsEventOperation eventOp, String eventName) {
    if (!(eventOp instanceof HopsEventOperationImpl))
      throw new IllegalArgumentException("The given event operation is not of a valid type: " +
              (eventOp != null ? eventOp.getClass().getSimpleName() : "null"));

    HopsEventOperationImpl eventOperationImpl = (HopsEventOperationImpl)eventOp;
    EventOperation clusterJEventOperation = eventOperationImpl.getClusterJEventOperation();

    EventOperationLifecycleManager.getInstance().deleteEventOperation(clusterJEventOperation, eventName);

    return session.dropEventOperation(clusterJEventOperation);
  }

  /**
   * Return the EventOperation associated with the next event that was received.
   *
   * This call will NOT work if:
   *  (1) execute() has not yet been called on the event operation, and
   *  (2) pollEvents() has not yet been called and indicated that at least one event has been received.
   *
   * @return EventOperation associated with the next event that was received.
   *         This will return null if there are no other events to receive.
   */
  public HopsEventOperation nextEvent() {
    EventOperation eventOperation = session.nextEvent();

    if (eventOperation == null) {
      LOG.warn("Underlying session object returned null EventOperation...");
      return null;
    }

    HopsEventOperation hopsEventOperation =
            EventOperationLifecycleManager.getInstance().getExistingInstance(eventOperation);

    // Since it was returned by nextEvent(), we can call true on this.
    // TODO: Should we set the flag to false once we're done processing this?
    //       We have to set it explicitly because we're reusing an old instance...
    HopsEventOperationImpl hopsEventOperationImpl = (HopsEventOperationImpl)hopsEventOperation;
    hopsEventOperationImpl.getClusterJEventOperation().setCanCallNextEvent(true);

    return hopsEventOperation;
  }

  /**
   * Create and register an NDB event with the server.
   *
   * Note that this event does not do anything without creating an event operation with it. Also note that
   * the event operation must have execute() called on it before it will begin reporting events.
   *
   * @param eventName The unique name to identify the event with.
   * @param tableName The table with which the event should be associated.
   * @param eventColumns The columns that are being monitored for the event.
   * @param tableEvents The events that this event should listen for.
   * @param recreateIfExists If true, then drop and recreate the event if it already exists.
   */
  public Event createAndRegisterEvent(
          String eventName, String tableName, String[] eventColumns,
          com.mysql.clusterj.TableEvent[] tableEvents, boolean recreateIfExists) throws StorageException {
    LOG.debug("Attempting to create and register event " + eventName + " now...");

    Event event;
    try {
      event = session.createAndRegisterEvent(eventName, tableName, eventColumns, tableEvents, 1, recreateIfExists);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }

    return event;
  }

  /**
   * Create and register an NDB event with the server.
   *
   * Note that this event does not do anything without creating an event operation with it. Also note that
   * the event operation must have execute() called on it before it will begin reporting events.
   *
   * When this version of the `createAndRegisterEvent()` function is used, all columns of the table are included
   * in the event.
   *
   * @param eventName The unique name to identify the event with.
   * @param tableName The table with which the event should be associated.
   * @param tableEvents The events that this event should listen for.
   * @param recreateIfExists If true, then drop and recreate the event if it already exists.
   */
  public Event createAndRegisterEvent(
          String eventName, String tableName, TableEvent[] tableEvents, boolean recreateIfExists)
          throws StorageException {
    LOG.debug("Attempting to create and register event " + eventName + " now...");

    Event event;
    try {
      event = session.createAndRegisterEvent(eventName, tableName, tableEvents, 1, recreateIfExists);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }

    return event;
  }

  /**
   * Create a subscription to an event defined in the database.
   *
   * Subscription will not become active until `execute()` is called on the returned object!
   *
   * @param eventName
   *        unique identifier of the event to subscribe to
   *
   * @return Object representing an event operation, NULL on failure
   */
  public HopsEventOperation createEventOperation(String eventName) throws StorageException {
    LOG.debug("Creating subscription on event '" + eventName + "' now...");

    return EventOperationLifecycleManager.getInstance().getOrCreateInstance(eventName, session);
  }

  public <T> HopsQuery<T> createQuery(HopsQueryDomainType<T> queryDefinition)
      throws StorageException {
    try {
      Query<T> query =
          session.createQuery(queryDefinition.getQueryDomainType());
      return new HopsQuery<>(query);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T find(Class<T> aClass, Object o) throws StorageException {
    try {
      return session.find(aClass, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T newInstance(Class<T> aClass) throws StorageException {
    try {
      return session.newInstance(aClass);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T newInstance(Class<T> aClass, Object o) throws StorageException {
    try {
      return session.newInstance(aClass, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T makePersistent(T t) throws StorageException {
    try {
      return session.makePersistent(t);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T load(T t) throws StorageException {
    try {
      return session.load(t);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Boolean found(Object o) throws StorageException {
    try {
      return session.found(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void persist(Object o) throws StorageException {
    try {
      session.persist(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Iterable<?> makePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      return session.makePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> void deletePersistent(Class<T> aClass, Object o)
      throws StorageException {
    try {
      session.deletePersistent(aClass, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void deletePersistent(Object o) throws StorageException {
    try {
      session.deletePersistent(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void remove(Object o) throws StorageException {
    try {
      session.remove(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> int deletePersistentAll(Class<T> aClass) throws StorageException {
    try {
      return session.deletePersistentAll(aClass);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void deletePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      session.deletePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void updatePersistent(Object o) throws StorageException {
    try {
      session.updatePersistent(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void updatePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      session.updatePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public <T> T savePersistent(T t) throws StorageException {
    try {
      return session.savePersistent(t);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Iterable<?> savePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      return session.savePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsTransaction currentTransaction() throws StorageException {
    try {
      Transaction transaction = session.currentTransaction();
      return new HopsTransaction(transaction);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void close() throws StorageException {
    try {
      session.close();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public boolean isClosed() throws StorageException {
    try {
      return session.isClosed();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void flush() throws StorageException {
    try {
      session.flush();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setPartitionKey(Class<?> aClass, Object o)
      throws StorageException {
    try {
      session.setPartitionKey(aClass, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setLockMode(LockMode lockMode) throws StorageException {
    try {
      session.setLockMode(lockMode);
      this.lockMode = lockMode;
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void markModified(Object o, String s) throws StorageException {
    try {
      session.markModified(o, s);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public String unloadSchema(Class<?> aClass) throws StorageException {
    try {
      return session.unloadSchema(aClass);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
  
  public <T> void release(T t)  throws StorageException {
    try {
      if(t!=null){
        session.release(t);
      }
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
  
  public <T> void release(Collection<T> t)  throws StorageException {
    try {
      if(t!=null){
        for(T dto : t)  {
          session.release(dto);
        }
      }
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public LockMode getCurrentLockMode(){
    return lockMode;
  }
}
