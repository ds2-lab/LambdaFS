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
package io.hops.metadata.common.entity;

import java.nio.ByteBuffer;

public class ByteArrayVariable extends Variable {

  private byte[] value;

  public ByteArrayVariable(Finder type, byte[] value) {
    this(type);
    if (value != null && value.length > 255) {
      throw new IllegalArgumentException(
          "byte array shouldn't exceed 255 bytes");
    }
    this.value = value;
  }

  public ByteArrayVariable(Finder type) {
    super(type);
  }

  public ByteArrayVariable(byte[] value) {
    this(Finder.GenericByteArray, value);
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setValue(byte[] val) {
    if (val.length == 0) {
      return;
    }
    ByteBuffer buf = ByteBuffer.wrap(val);
    int len = buf.get();
    if(len==0){
      return;
    }
    value = new byte[len];
    buf.get(value);
  }

  @Override
  public byte[] getBytes() {
    int length = getLength();
    if (length > 0) {
      ByteBuffer buf = ByteBuffer.allocate(getLength());
      buf.put((byte) value.length);
      buf.put(value);
      return buf.array();
    } else {
      ByteBuffer buf = ByteBuffer.allocate(1);
      buf.put((byte) 0);
      return buf.array();
    }
  }

  @Override
  public int getLength() {
    if (value == null) {
      return -1;
    }
    return value.length + 1;
  }

  protected void setByteArrayValue(byte[] val) {
    this.value = val;
  }

  protected byte[] getByteArrayValue() {
    return this.value;
  }
}
