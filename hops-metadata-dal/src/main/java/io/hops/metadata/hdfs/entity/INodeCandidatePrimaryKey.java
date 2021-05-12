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

public class INodeCandidatePrimaryKey
    implements Comparable<INodeCandidatePrimaryKey> {

  long inodeId;

  public INodeCandidatePrimaryKey(long inodeId) {
    this.inodeId = inodeId;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof INodeCandidatePrimaryKey) {
      INodeCandidatePrimaryKey other = (INodeCandidatePrimaryKey) obj;
      if (this.inodeId == other.inodeId) {
        return true;
      }
    }
    return false;
  }


  @Override
  public String toString() {
    return "Id:" + inodeId;
  }


  @Override
  public int hashCode() {
    int hash = 7;
    hash = 59 * hash + Long.hashCode(this.inodeId);
    return hash;
  }


  @Override
  public int compareTo(INodeCandidatePrimaryKey t) {
    if (this.equals(t)) {
      return 0;
    }

    if (this.getInodeId() > t.getInodeId()) {
      return 1;
    } else {
      return -1;
    }
  }


}
