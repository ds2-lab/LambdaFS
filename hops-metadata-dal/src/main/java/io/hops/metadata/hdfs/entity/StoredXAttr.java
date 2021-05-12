/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.hdfs.entity;

import com.google.common.base.Charsets;
import io.hops.metadata.common.FinderType;

import java.util.Arrays;
import java.util.Objects;

public final class StoredXAttr {
  
  public static final int MAX_NUM_USER_XATTRS_PER_INODE = 127;
  public static final int MAX_NUM_SYS_XATTRS_PER_INODE = 127;
  public static final int MAX_XATTR_NAME_SIZE = 255;
  public static final int MAX_XATTR_VALUE_ROW_SIZE = 13500;
  public static final int MAX_XATTR_MAX_NUM_PARTS = 255;
  public static final int MAX_XATTR_VALUE_SIZE =
      MAX_XATTR_MAX_NUM_PARTS * MAX_XATTR_VALUE_ROW_SIZE;
  
  public enum Finder implements FinderType<StoredXAttr> {
    ByPrimaryKey,
    ByPrimaryKeyLocal,
    ByPrimaryKeyBatch,
    ByInodeId,
    ByInodeIdLocal;
    
    @Override
    public Class getType() {
      return StoredXAttr.class;
    }
    
    @Override
    public Annotation getAnnotated() {
      switch (this){
        case ByPrimaryKey:
        case ByPrimaryKeyLocal:
          return Annotation.PrimaryKey;
        case ByPrimaryKeyBatch:
          return Annotation.Batched;
        case ByInodeId:
        case ByInodeIdLocal:
          return Annotation.PrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  public final static class PrimaryKey{
    private final long inodeId;
    private final byte namespace;
    private final String name;
  
    public PrimaryKey(long inodeId, byte namespace, String name) {
      this.inodeId = inodeId;
      this.namespace = namespace;
      this.name = name;
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
          name.equals(that.name);
    }
  
    @Override
    public int hashCode() {
      return Objects.hash(inodeId, namespace, name);
    }
  
    @Override
    public String toString() {
      return "PrimaryKey{" +
          "inodeId=" + inodeId +
          ", namespace=" + namespace +
          ", name='" + name + '\'' +
          '}';
    }
  }
  
  private final PrimaryKey primaryKey;
  private final byte[] value;
  private short oldNumParts = -1;
  
  public StoredXAttr(long inodeId, byte namespace, String name, byte[] value) {
    this.primaryKey = new PrimaryKey(inodeId, namespace, name);
    this.value = value;
  }
  
  public long getInodeId() {
    return primaryKey.getInodeId();
  }
  
  public byte getNamespace() {
    return primaryKey.getNamespace();
  }
  
  public String getName() {
    return primaryKey.getName();
  }
  
  public byte[] getValue() {
    return value;
  }
  
  public PrimaryKey getPrimaryKey(){
    return primaryKey;
  }
  
  public short getOldNumParts() { return oldNumParts; }
  
  public void setOldNumParts(short oldNumParts){
    this.oldNumParts = oldNumParts;
  }
  
  public final static byte[] getXAttrBytes(String val){
    if(val == null)
      return null;
    return val.getBytes(Charsets.UTF_8);
  }
  
  public final static String getXAttrString(byte[] val){
    if(val == null)
      return null;
    return new String(val,Charsets.UTF_8);
  }
  
  public short getNumParts(){
    return getNumParts(value);
  }
  public byte[] getValue(short index) {
    return getValue(value, index);
  }
  
  public static short getNumParts(byte[] value){
    if(value == null || value.length == 0)
      return 1;
    return (short) Math.ceil(value.length / ((double) MAX_XATTR_VALUE_ROW_SIZE));
  }
  
  public static byte[] getValue(byte[] value, short index){
    if(value == null)
      return null;
    int byteWritten = index * MAX_XATTR_VALUE_ROW_SIZE;
    int toWrite = value.length - byteWritten;
    if(toWrite > MAX_XATTR_VALUE_ROW_SIZE){
      toWrite = MAX_XATTR_VALUE_ROW_SIZE;
    }
    byte[] buffer = new byte[toWrite];
    System.arraycopy(value, byteWritten, buffer, 0, toWrite);
    return buffer;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoredXAttr)) {
      return false;
    }
    StoredXAttr that = (StoredXAttr) o;
    return primaryKey.equals(that.primaryKey) &&
        Arrays.equals(getValue(), that.getValue());
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(primaryKey, getValue());
  }
  
  @Override
  public String toString() {
    return "StoredXAttr{" +
        "primaryKey=" + primaryKey +
        ", value='" + value + '\'' +
        '}';
  }
}
