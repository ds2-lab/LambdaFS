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

import java.io.UnsupportedEncodingException;

public class StringVariable extends ByteArrayVariable {

  private String value = null;

  public StringVariable(Finder type, String value) {
    this(type);
    if (value.length() > 255) {
      throw new IllegalArgumentException(
          "string variables shouldn't exceed 255 bytes");
    }
    this.value = value;
  }

  public StringVariable(Finder type) {
    super(type);
  }

  public StringVariable(String value) {
    this(Finder.GenericString, value);
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public void setValue(byte[] val) {
    super.setValue(val);
    value = getString(super.getByteArrayValue());
  }

  @Override
  public byte[] getBytes() {
    byte[] s = getByteArray(value);
    super.setByteArrayValue(s);
    return super.getBytes();
  }

  @Override
  public int getLength() {
    if (value == null) {
      return -1;
    }
    return getByteArray(value).length + 1;
  }

  private static byte[] getByteArray(String val) {
    try {
      return val.getBytes("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String getString(byte[] val) {
    try {
      return new String(val, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public String toString() {
    return value;
  }
  
}
