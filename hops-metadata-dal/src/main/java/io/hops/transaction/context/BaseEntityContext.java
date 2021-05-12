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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

abstract class BaseEntityContext<Key, Entity> extends EntityContext<Entity> {

  private static boolean statsEnabled = false;

  public static void enableStats() {
    statsEnabled = true;
  }

  public static void disableStats() {
    statsEnabled = false;
  }

  enum State {
    DBREAD,
    ADDED,
    MODIFIED,
    REMOVED
  }

  class ContextEntity {
    private State firstState;
    private State state;
    private Entity entity;

    ContextEntity(Entity entity) {
      this.entity = entity;
    }

    ContextEntity(Entity entity, State state) {
      this(entity);
      this.firstState = state;
      this.state = state;
    }

    void update(Entity entity, State state) {
      this.entity = entity;
      this.state = state;
    }

    State getState() {
      return state;
    }

    Entity getEntity() {
      return entity;
    }

    @Override
    public String toString() {
      return "ContextEntity{" +
          "state=" + state +
          ", entity=" + entity +
          '}';
    }
  }

  private final Map<Key, ContextEntity> contextEntities =
      new HashMap<>();

  private EntityContextStat contextStat;

  @Override
  public void add(Entity entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void update(Entity entity) throws TransactionContextException {
    Key entityKey = getKey(entity);
    ContextEntity contextEntity = contextEntities.get(entityKey);
    if (contextEntity == null) {
      contextEntity = new ContextEntity(entity, State.ADDED);
      contextEntities.put(entityKey, contextEntity);
    } else {
      contextEntity.update(entity, State.MODIFIED);
    }
  }

  @Override
  public void remove(Entity entity) throws TransactionContextException {
    Key entityKey = getKey(entity);
    ContextEntity contextEntity = contextEntities.get(entityKey);
    if (contextEntity != null && contextEntity.getEntity() != null) {
      contextEntity.update(entity, State.REMOVED);
    } else {
      throw new TransactionContextException(
          "Unattached Entity passed to be removed " + entity);
    }
  }
  
  @Override
  public void clear() throws TransactionContextException {
    storageCallPrevented = false;
    contextEntities.clear();
  }

  @Override
  public int count(CounterType<Entity> counter, Object... params)
      throws TransactionContextException, StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entity find(FinderType<Entity> finder, Object... params)
      throws TransactionContextException, StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Collection<Entity> findList(FinderType<Entity> finder,
      Object... params) throws TransactionContextException, StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }


  @Override
  public void removeAll() throws TransactionContextException, StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public final EntityContextStat collectSnapshotStat()
      throws TransactionContextException {
    return resetContextStat();
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {

  }

  final Collection<Entity> getRemoved() {
    Collection<Entity> entities = Maps.transformValues(contextEntities,
        new Function<ContextEntity, Entity>() {
          @Override
          public Entity apply(ContextEntity input) {
            if (input.getState() == State.REMOVED &&
                input.firstState == State.DBREAD) {
              return input.getEntity();
            }
            return null;
          }
        }).values();
    return Collections2.filter(entities, Predicates.notNull());
  }

  final Collection<Entity> getAdded() {
    return filterValuesOnState(State.ADDED);
  }

  final Collection<Entity> getModified() {
    return filterValuesOnState(State.MODIFIED);
  }

  final Collection<Entity> filterValuesOnState(final State state) {
    return filterValues(state, false);
  }

  final Collection<Entity> filterValuesNotOnState(final State state) {
    return filterValues(state, true);
  }

  private Collection<Entity> filterValues(final State state,
      final boolean not) {
    Collection<Entity> entities = Maps.transformValues(contextEntities,
        new Function<ContextEntity, Entity>() {
          @Override
          public Entity apply(ContextEntity input) {
            if (not ? (input.getState() != state) :
                (input.getState() == state)) {
              return input.getEntity();
            }
            return null;
          }
        }).values();
    return Collections2.filter(entities, Predicates.notNull());
  }

  abstract Key getKey(Entity entity);

  final boolean isNewlyAdded(Key key) {
    return containsFirst(key, State.ADDED);
  }

  final boolean isReadFromDB(Key key) {
    return containsFirst(key, State.DBREAD);
  }

  final boolean isRemoved(Key key) {
    return contains(key, State.REMOVED);
  }

  final boolean isModified(Key key) {
    return contains(key, State.MODIFIED);
  }

  private boolean containsFirst(Key key, State state) {
    ContextEntity ce = contextEntities.get(key);
    if (ce != null) {
      return ce.firstState == state;
    }
    return false;
  }

  private boolean contains(Key key, State state) {
    ContextEntity ce = contextEntities.get(key);
    if (ce != null) {
      return ce.getState() == state;
    }
    return false;
  }

  final boolean contains(Key key) {
    return contextEntities.containsKey(key);
  }

  final boolean contains(Predicate<ContextEntity> pred) {
    return get(pred).size() != 0;
  }

  final Entity get(Key key) {
    ContextEntity ce = contextEntities.get(key);
    if (ce != null && ce.getState() != State.REMOVED) {
      return ce.getEntity();
    }
    return null;
  }

  final Collection<Entity> get(Predicate<ContextEntity> pred) {
    Map<Key, ContextEntity> filtered = Maps.filterValues(contextEntities, pred);
    Collection<Entity> transformed =
        Maps.transformValues(filtered, new Function<ContextEntity, Entity>() {
              @Override
              public Entity apply(ContextEntity input) {
                return input.getEntity();
              }
            }).values();
    return Collections2.filter(transformed, Predicates.notNull());
  }

  final Collection<Entity> getAll() {
    Collection<Entity> tr = Collections2.transform(contextEntities.values(),
        new Function<ContextEntity, Entity>() {
          @Override
          public Entity apply(ContextEntity input) {
            return input.getEntity();
          }
        });
    return Collections2.filter(tr, Predicates.notNull());
  }

  final ContextEntity max(Comparator<ContextEntity> contextEntityComparator) {
    return Collections.max(contextEntities.values(), contextEntityComparator);
  }

  void gotFromDB(Collection<Entity> entityList) {
    if (entityList != null) {
      for (Entity entity : entityList) {
        gotFromDB(entity);
      }
    }
  }

  final void gotFromDB(Entity entity) {
    if (entity != null) {
      gotFromDB(getKey(entity), entity);
    }
  }

  void gotFromDB(Key entityKey, Entity entity) {
    ContextEntity contextEntity = contextEntities.get(entityKey);
    if (contextEntity == null) {
      contextEntity = new ContextEntity(entity, State.DBREAD);
      contextEntities.put(entityKey, contextEntity);
    } else {
      //if(contextEntity.getEntity() == null){
      contextEntity.update(entity, State.DBREAD);
      //}
    }
  }

  final int size() {
    return contextEntities.size();
  }

  protected void hit(FinderType finder, Entity res, Object... params) {
    hit(finder, res == null ? 0 : 1, params);
  }

  protected void hit(FinderType finder, Collection<Entity> res,
      Object... params) {
    hit(finder, res == null ? 0 : res.size(), params);
  }

  protected void miss(FinderType finder, Entity res, Object... params) {
    miss(finder, res == null ? 0 : 1, params);
  }

  protected void miss(FinderType finder, Collection<Entity> res,
      Object... params) {
    miss(finder, res == null ? 0 : res.size(), params);
  }

  protected void missUpgrade(FinderType finder, Entity res, Object... params) {
    missUpgrade(finder, res == null ? 0 : 1, params);
  }

  protected void missUpgrade(FinderType finder, Collection<Entity> res,
      Object... params) {
    missUpgrade(finder, res == null ? 0 : res.size(), params);
  }


  private void hit(FinderType finder, int count, Object... params) {
    log(finder, CacheHitState.HIT, params);
    hit(finder, count);
  }

  private void miss(FinderType finder, int count, Object... params) {
    miss(finder, CacheHitState.LOSS, count, params);
  }

  private void missUpgrade(FinderType finder, int count, Object... params) {
    miss(finder, CacheHitState.LOSS_LOCK_UPGRADE, count, params);
  }

  private void miss(FinderType finder, CacheHitState state, int count, Object
      ... params) {
    log(finder, state, params);
    miss(finder, count);
  }


  private void hit(FinderType finderType, int count) {
    if (statsEnabled) {
      getContextStat().hit(finderType, count);
    }
  }

  private void miss(FinderType finderType, int count) {
    if (statsEnabled) {
      getContextStat().miss(finderType, count);
    }
  }

  private EntityContextStat getContextStat() {
    if (contextStat == null) {
      contextStat = new EntityContextStat(this.getClass().getSimpleName());
    }
    return contextStat;
  }

  private EntityContextStat resetContextStat() {
    if (statsEnabled) {
      EntityContextStat stat = getContextStat();
      stat.commited(getAdded().size(), getModified().size(),
          getRemoved().size());
      contextStat = null;
      if (stat.isEmpty()) {
        stat = null;
      }
      return stat;
    }
    return null;
  }

}
