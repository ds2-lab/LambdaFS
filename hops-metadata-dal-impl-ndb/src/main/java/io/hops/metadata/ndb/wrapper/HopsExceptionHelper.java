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

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import io.hops.exception.*;

public class HopsExceptionHelper {
  public static StorageException wrap(ClusterJException e) {
    if (isTransient(e)) {
      if(isDeadLockException(e)){
        return new TransientDeadLockException(e);
      }else {
        return new TransientStorageException(e);
      }
    } else if (isTupleAlreadyExisted(e)) {
      return new TupleAlreadyExistedException(e);
    } else if(isForeignKeyConstraintViolation(e)){
      return new ForeignKeyConstraintViolationException(e);
    } else if(isUniqueKeyConstraintViolation(e)){
      return new UniqueKeyConstraintViolationException(e);
    } else if (isOutOfDBExtents(e)) {
      return new OutOfDBExtentsException(e);
    } else {
      return new StorageException(e);
    }
  }

  private static boolean isTransient(ClusterJException ex) {
    ClusterJException e = ex;
    if (!(e instanceof ClusterJDatastoreException) && e.getCause() != null
        && e.getCause() instanceof ClusterJDatastoreException) {
      e = (ClusterJException) e.getCause();
    }
    if (e instanceof ClusterJDatastoreException) {
      // http://dev.mysql.com/doc/ndbapi/en/ndb-error-classifications.html
      // The classifications can be found in ndberror.h and ndberror.c in the ndb sources
      int classification = ((ClusterJDatastoreException) e).getClassification();
      if (classification == ClusterJDatastoreException.Classification.TemporaryResourceError.value || 
          classification == ClusterJDatastoreException.Classification.NodeRecoveryError.value || 
          classification == ClusterJDatastoreException.Classification.OverloadError.value || 
          classification == ClusterJDatastoreException.Classification.TimeoutExpired.value || 
          classification == ClusterJDatastoreException.Classification.NodeShutdown.value || 
          classification == ClusterJDatastoreException.Classification.InternalTemporary.value) {
        return true;
      }
    }
    return false;
  }

  private static boolean isTupleAlreadyExisted(ClusterJException e) {
    return isExceptionContains(e, 630);
  }

  private static boolean isForeignKeyConstraintViolation(ClusterJException e){
    return isExceptionContains(e, 255);
  }

  private static boolean isUniqueKeyConstraintViolation(ClusterJException e){
    return isExceptionContains(e, 893);
  }

  private static boolean isOutOfDBExtents(ClusterJException e){
    return isExceptionContains(e, 1601);
  }

  private static boolean isDeadLockException(ClusterJException e){
    return isExceptionContains(e, 266) || isExceptionContains(e, 274);
  }

  private static boolean isExceptionContains(ClusterJException e, int code){
    if (e instanceof  ClusterJDatastoreException) {
      if (((ClusterJDatastoreException)e).getCode() == code) {
        return true;
      }
    }
    return false;
  }
}
