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
import com.mysql.clusterj.query.PredicateOperand;
import io.hops.exception.StorageException;

public class HopsPredicateOperand {
  private final PredicateOperand predicateOperand;

  public HopsPredicateOperand(PredicateOperand predicateOperand) {
    this.predicateOperand = predicateOperand;
  }

  public HopsPredicate equal(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(
          this.predicateOperand.equal(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate greaterThan(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand
          .greaterThan(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate greaterEqual(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand
          .greaterEqual(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate lessThan(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand
          .lessThan(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate lessEqual(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand
          .lessEqual(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate between(HopsPredicateOperand predicateOperand,
      HopsPredicateOperand predicateOperand1) throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand
          .between(predicateOperand.getPredicateOperand(),
              predicateOperand1.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate in(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(
          this.predicateOperand.in(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate like(HopsPredicateOperand predicateOperand)
      throws StorageException {
    try {
      return new HopsPredicate(
          this.predicateOperand.like(predicateOperand.getPredicateOperand()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate isNull() throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand.isNull());
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate isNotNull() throws StorageException {
    try {
      return new HopsPredicate(this.predicateOperand.isNotNull());
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  PredicateOperand getPredicateOperand() {
    return predicateOperand;
  }
}
