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

import io.hops.metadata.common.FinderType;

import java.util.Comparator;

public class Ace {
  public enum AceType {
    USER(0),
    GROUP(1),
    MASK(2),
    OTHER(3);
    
    int value;
    
    AceType(int value){
      this.value = value;
    }
  
    public int getValue() {
      return value;
    }
    
    public static AceType valueOf(int value){
      switch (value){
        case 0:
          return USER;
        case 1:
          return GROUP;
        case 2:
          return MASK;
        case 3:
          return OTHER;
        default:
          throw new RuntimeException("Incorrect value " + value + ", should be 0, 1, 2 or 3;");
      }
    }
  }
  public enum Order implements Comparator<Ace> {
    
    ByIndexAscending() {
      @Override
      public int compare(Ace ace, Ace t1) {
        return ace.getIndex()-t1.getIndex();
      }
    }
  }
  
  public enum Finder implements FinderType<Ace> {
    ByInodeIdAndIndices;
    
    @Override
    public Class getType() {
      return Ace.class;
    }
    
    @Override
    public Annotation getAnnotated() {
      switch (this){
        case ByInodeIdAndIndices:
          return Annotation.PrimaryKey;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  public static class PrimaryKey{
    public final long inodeId;
    public final int index;
    
    public PrimaryKey(long inodeId, int index){
      this.index = index;
      this.inodeId = inodeId;
    }
  
    @Override
    public int hashCode() {
      //todo what is the go to hash mechanism that is consistent with equals?
      return index * Long.hashCode(inodeId);
    }
  
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PrimaryKey){
        PrimaryKey other = (PrimaryKey) obj;
        return inodeId == other.inodeId && index == other.index;
      }
      return false;
    }
  }
  
  private long inodeId;
  private int index;
  private String subject;
  private AceType type;
  private boolean isDefault;
  private int permission;
  
  public Ace(long inodeId, int index){
    this.inodeId = inodeId;
    this.index = index;
  }
  
  public Ace(long inodeId, int index, String subject, AceType type, boolean isDefault, int permission) {
    this.inodeId = inodeId;
    this.index = index;
    this.subject = subject;
    this.isDefault = isDefault;
    this.permission = permission;
    this.type = type;
  }
  

  public long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(long inodeId){
    this.inodeId = inodeId;
  }
  
  public String getSubject() {
    return subject;
  }
  
  public AceType getType() {
    return type;
  }
  
  public boolean isDefault() {
    return isDefault;
  }
  
  public void setIsDefault(boolean isDefault){
    this.isDefault = isDefault;
  }
  
  public int getPermission() {
    return permission;
  }
  
  public int getIndex() {return index;}
  
  public Ace copy(){
    return new Ace(inodeId,index,subject,type,isDefault,permission);
  }
}
