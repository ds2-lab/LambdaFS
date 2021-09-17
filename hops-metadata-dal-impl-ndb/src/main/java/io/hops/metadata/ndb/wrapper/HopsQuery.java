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

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Results;
import io.hops.exception.StorageException;

import java.util.List;
import java.util.Map;

public class HopsQuery<E> {
  private final Query<E> query;

  public HopsQuery(Query<E> query) {
    this.query = query;
  }

  public void setParameter(String s, Object o) throws StorageException {
    try {
      query.setParameter(s, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public List<E> getResultList() throws StorageException {
    try {
      return query.getResultList();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public int deletePersistentAll() throws StorageException {
    try {
      return query.deletePersistentAll();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Object o) throws StorageException {
    try {
      return query.execute(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Object... objects) throws StorageException {
    try {
      return query.execute(objects);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Map<String, ?> map) throws StorageException {
    try {
      return query.execute(map);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Map<String, Object> explain() throws StorageException {
    try {
      return query.explain();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setLimits(long l, long l1) throws StorageException {
    try {
      query.setLimits(l, l1);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setOrdering(Query.Ordering ordering, String... strings)
      throws StorageException {
    try {
      query.setOrdering(ordering, strings);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
