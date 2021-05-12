/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2020 Logical Clocks AB
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
package io.hops.metadata.hdfs.entity;

import io.hops.metadata.common.FinderType;

public class LeaseCreationLock {

  public static enum Finder implements FinderType<LeaseCreationLock> {

    ByRowID;

    @Override
    public Class getType() {
      return LeaseCreationLock.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByRowID:
          return Annotation.PrimaryKey;
        default:
          throw new IllegalStateException();
      }
    }

  }

  public LeaseCreationLock(int lock) {
    this.lock = lock;
  }

  public int getLock() {
    return lock;
  }

  public void setLock(int lock) {
    this.lock = lock;
  }

  private int lock;
}
