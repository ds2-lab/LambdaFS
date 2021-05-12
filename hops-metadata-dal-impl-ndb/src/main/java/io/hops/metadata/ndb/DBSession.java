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

import io.hops.metadata.ndb.wrapper.HopsSession;

public class DBSession {

  private HopsSession session;
  private final int MAX_REUSE_COUNT;
  private int sessionUseCount;

  public DBSession(HopsSession session, int maxReuseCount) {
    this.session = session;
    this.MAX_REUSE_COUNT = maxReuseCount;
    this.sessionUseCount = 0;
  }

  public HopsSession getSession() {
    return session;
  }

  public int getSessionUseCount() {
    return sessionUseCount;
  }

  public void setSessionUseCount(int sessionUseCount) {
    this.sessionUseCount = sessionUseCount;
  }

  public int getMaxReuseCount() {
    return MAX_REUSE_COUNT;
  }
}
