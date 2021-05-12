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
package io.hops.metadata.hdfs.entity;

public final class Lease implements Comparable<Lease> {

  private String holder;
  private long lastUpdate;
  private int holderId;
  private int count;

  public Lease() {
  }

  public Lease(String holder, int holderId, long lastUpdate, int count) {
    this.holder = holder;
    this.holderId = holderId;
    this.lastUpdate = lastUpdate;
    this.count = count;
  }

  public String getHolder() {
    return holder;
  }

  public void setHolder(String holder) {
    this.holder = holder;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public int getHolderId() {
    return holderId;
  }

  public void setHolderId(int holderId) {
    this.holderId = holderId;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public int compareTo(Lease o) {
    Lease l1 = this;
    Lease l2 = o;
    long lu1 = l1.lastUpdate;
    long lu2 = l2.lastUpdate;
    if (lu1 < lu2) {
      return -1;
    } else if (lu1 > lu2) {
      return 1;
    } else {
      return l1.holder.compareTo(l2.holder);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Lease)) {
      return false;
    }
    Lease obj = (Lease) o;
    if (lastUpdate == obj.lastUpdate && holder.equals(obj.holder)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return holder.hashCode();
  }
}
