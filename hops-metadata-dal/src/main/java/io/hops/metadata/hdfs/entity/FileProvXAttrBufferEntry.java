/*
 * Copyright (C) 2019 hops.io.
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

import java.util.Objects;

public final class FileProvXAttrBufferEntry {
  public final static class PrimaryKey{
    private final long inodeId;
    private final byte namespace;
    private final String name;
    private final int iNodeLogicalTime;
    
    public PrimaryKey(long inodeId, byte namespace, String name, int iNodeLogicalTime) {
      this.inodeId = inodeId;
      this.namespace = namespace;
      this.name = name;
      this.iNodeLogicalTime = iNodeLogicalTime;
    }
    
    public long getInodeId() {
      return inodeId;
    }
    
    public byte getNamespace() {
      return namespace;
    }
    
    public String getName() {
      return name;
    }
  
    public int getiNodeLogicalTime() {
      return iNodeLogicalTime;
    }
  
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PrimaryKey)) {
        return false;
      }
      PrimaryKey that = (PrimaryKey) o;
      return inodeId == that.inodeId &&
        namespace == that.namespace &&
        name.equals(that.name) &&
        iNodeLogicalTime == that.iNodeLogicalTime;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(inodeId, namespace, name, iNodeLogicalTime);
    }
    
    @Override
    public String toString() {
      return "FileProvXAttrBuffer.PrimaryKey{" +
        "inodeId=" + inodeId +
        ", namespace=" + namespace +
        ", name='" + name +
        ", iNodeLogicalTime='" + iNodeLogicalTime + '\'' +
        '}';
    }
  }

  private final PrimaryKey key;
  private final byte[] value;

  public FileProvXAttrBufferEntry(long iNodeId, byte namespace, String name, int iNodeLogicalTime, byte[] value) {
    this.key = new PrimaryKey(iNodeId, namespace, name, iNodeLogicalTime);
    this.value = value;
  }

  public FileProvXAttrBufferEntry(long inodeId, byte namespace, String name, int inodeLogicalTime, String value) {
    this(inodeId, namespace, name, inodeLogicalTime, StoredXAttr.getXAttrBytes(value));
  }

  public long getInodeId() {
    return key.getInodeId();
  }

  public byte getNamespace() {
    return key.getNamespace();
  }

  public String getName() {
    return key.getName();
  }

  public int getINodeLogicalTime() {
    return key.getiNodeLogicalTime();
  }

  public byte[] getValue() {
    return value;
  }
  
  public short getNumParts(){
    return StoredXAttr.getNumParts(value);
  }
  public byte[] getValue(short index) {
    return StoredXAttr.getValue(value, index);
  }
  
  @Override
  public String toString() {
    return "FileProvXAttrBufferEntry{"
      + "key='" + key + '\''
      + ", value='" + StoredXAttr.getXAttrString(value) + '\''
      + '}';
  }
}
