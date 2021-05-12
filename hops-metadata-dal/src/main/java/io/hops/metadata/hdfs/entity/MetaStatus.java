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

public enum MetaStatus {
  DISABLED((byte)0),
  META_ENABLED((byte)1),
  MIN_PROV_ENABLED((byte)2),
  FULL_PROV_ENABLED((byte)3);
  byte val;
  MetaStatus(byte val) {
    this.val = val;
  }
  
  public static MetaStatus fromVal(byte val) {
    switch(val) {
      case 1: return META_ENABLED;
      case 2: return MIN_PROV_ENABLED;
      case 3: return FULL_PROV_ENABLED;
      case 0:
      default : return DISABLED;
    }
  }
  public byte getVal(){
    return val;
  }
  public boolean isMetaEnabled() {
    switch(this) {
      case META_ENABLED:
      case MIN_PROV_ENABLED:
      case FULL_PROV_ENABLED:
        return true;
      case DISABLED:
      default :
        return false;
    }
  }
}
