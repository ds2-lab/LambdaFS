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
package io.hops.metadata.ndb;

import com.mysql.ndbjtie.ndbapi.NdbDictionary;

public final class NdbBoolean {
  public static final byte TRUE = 1;
  public static final byte FALSE = 0;

  public static byte convert(boolean val) {
    if (val) {
      return TRUE;
    } else {
      return FALSE;
    }
  }

  /**
   * Booleans are stored in intermediate storage as bytes (in the case of NDB). This allows us to convert
   * between the NDB-representation of a boolean and the Java representation.
   * @param val The byte value from NDB representing a boolean value.
   * @return The boolean value of the byte.
   */
  public static boolean convert(byte val) {
    if (val == TRUE) {
      return true;
    }
    if (val == FALSE) {
      return false;
    }
    throw new IllegalArgumentException();
  }
}
