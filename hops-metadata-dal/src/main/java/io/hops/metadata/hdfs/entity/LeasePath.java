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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;

public class LeasePath implements Comparable<LeasePath> {

  public static enum Finder implements FinderType<LeasePath> {

    ByHolderId,
    ByPath,
    ByPrefix;

    @Override
    public Class getType() {
      return LeasePath.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByHolderId:
          return Annotation.PrunedIndexScan;
        case ByPath:
          return Annotation.IndexScan;
        case ByPrefix:
          return Annotation.IndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private int holderId;
  private String path;

  private long lastBlockId;
  private long penultimateBlockId;

  public LeasePath(String path, int holderId) {
    this.holderId = holderId;
    this.path = path;
    this.lastBlockId = -1;
    this.penultimateBlockId = -1;
  }

  public LeasePath(String path, int holderId, long lastBlockId, long
      penultimateBlockId){
    this(path, holderId);
    this.lastBlockId = lastBlockId;
    this.penultimateBlockId = penultimateBlockId;
  }
  /**
   * @return the holderId
   */
  public int getHolderId() {
    return holderId;
  }

  /**
   * @param holderId
   *     the holderId to set
   */
  public void setHolderId(int holderId) {
    this.holderId = holderId;
  }

  /**
   * @return the path
   */
  public String getPath() {
    return path;
  }

  /**
   * @param path
   *     the path to set
   */
  public void setPath(String path) {
    this.path = path;
  }

  public long getLastBlockId() {
    return lastBlockId;
  }

  public void setLastBlockId(long lastBlockId) {
    this.lastBlockId = lastBlockId;
  }

  public long getPenultimateBlockId() {
    return penultimateBlockId;
  }

  public void setPenultimateBlockId(long penultimateBlockId) {
    this.penultimateBlockId = penultimateBlockId;
  }

  @Override
  public int compareTo(LeasePath t) {
    return this.path.compareTo(t.getPath());
  }

  @Override
  public boolean equals(Object obj) {
    if(obj==null || !(obj instanceof LeasePath)){
      return false;
    }
    LeasePath other = (LeasePath) obj;
    return (this.path.equals(other.getPath()) &&
        this.holderId == other.getHolderId());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.path != null ? this.path.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return this.path;
  }

  public void deletePersistent() throws TransactionContextException, StorageException {
    EntityManager.remove(this);
  }

  public void savePersistent() throws TransactionContextException, StorageException {
    EntityManager.update(this);
  }
}
