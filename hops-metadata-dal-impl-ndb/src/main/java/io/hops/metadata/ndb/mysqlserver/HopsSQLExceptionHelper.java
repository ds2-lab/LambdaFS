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
package io.hops.metadata.ndb.mysqlserver;

import com.mysql.jdbc.MysqlErrorNumbers;
import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

public class HopsSQLExceptionHelper {
  public static StorageException wrap(SQLException e) {
    if (isTransient(e)) {
      return new TransientStorageException(e);
    } else {
      return new StorageException(e);
    }
  }

  private static boolean isTransient(SQLException e) {
    if (e instanceof SQLTransientException) {
      return true;
    } else if (e instanceof SQLRecoverableException) {
      return true;
    } else if (e instanceof SQLNonTransientException) {
      return false;
    } else if (e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT) {
      //Lock wait timeout exceeded; try restarting transaction
      return true;
    } else if (e.getErrorCode() == MysqlErrorNumbers.ER_GET_TEMPORARY_ERRMSG) {
      //temporary error, try again
      return true;
    }
    return false;
  }
}
