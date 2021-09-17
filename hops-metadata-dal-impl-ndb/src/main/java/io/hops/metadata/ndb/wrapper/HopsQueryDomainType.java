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
import com.mysql.clusterj.query.QueryDomainType;
import io.hops.exception.StorageException;

public class HopsQueryDomainType<E> {
  private final QueryDomainType<E> queryDomainType;

  public HopsQueryDomainType(QueryDomainType<E> queryDomainType) {
    this.queryDomainType = queryDomainType;
  }

  public HopsPredicateOperand get(String s) throws StorageException {
    try {
      return new HopsPredicateOperand(queryDomainType.get(s));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Class<E> getType() throws StorageException {
    try {
      return queryDomainType.getType();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsQueryDefinition<E> where(HopsPredicate predicate)
      throws StorageException {
    try {
      return new HopsQueryDefinition<>(
          queryDomainType.where(predicate.getPredicate()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicateOperand param(String s) throws StorageException {
    try {
      return new HopsPredicateOperand(queryDomainType.param(s));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate not(HopsPredicate predicate) throws StorageException {
    try {
      return new HopsPredicate(queryDomainType.not(predicate.getPredicate()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  QueryDomainType<E> getQueryDomainType() {
    return queryDomainType;
  }
}
