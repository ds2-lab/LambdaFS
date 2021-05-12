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

/**
 * A policy which describes an erasure coding configuration. It specifies the
 * requested erasure coding codec and the replication factor to be applied
 * after a file was encoded.
 *
 * Possible codecs included in this distribution are listed in the
 * erasure-coding-default.xml configuration file. Custom codecs can be configured
 * by creating a erasure-coding-site.xml in the config directory.
 */
public class EncodingPolicy {
  private String codec;
  private short targetReplication;

  /**
   * Construct and EncodingPolicy.
   *
   * @param codec
   *    the id of codec
   * @param targetReplication
   *    the replication factor to be applied after the encoding
   */
  public EncodingPolicy(String codec, short targetReplication) {
    setCodec(codec);
    setTargetReplication(targetReplication);
  }

  /**
   * Set the codec.
   *
   * @param codec
   *    the id of codec
   */
  public void setCodec(String codec) {
    if (codec.length() > 8) {
      throw new IllegalArgumentException(
          "Codec cannot have more than 8 characters");
    }
    this.codec = codec;
  }

  /**
   * Set the replication factor to be applied after the encoding.
   *
   * @param targetReplication
   *    the replication factor to be applied after the encoding
   */
  public void setTargetReplication(short targetReplication) {
    this.targetReplication = targetReplication;
  }

  /**
   * Get the codec id.
   * @return
   *  the id of the codec
   */
  public String getCodec() {
    return codec;
  }

  /**
   * Get the replication factor to be applied after the encoding.
   *
   * @return
   *    the replication factor to be applied after the encoding
   */
  public short getTargetReplication() {
    return targetReplication;
  }

  @Override
  public String toString() {
    return "EncodingPolicy{" +
        "codec='" + codec + '\'' +
        ", targetReplication=" + targetReplication +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EncodingPolicy policy = (EncodingPolicy) o;

    if (targetReplication != policy.targetReplication) {
      return false;
    }
    if (!codec.equals(policy.codec)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = codec.hashCode();
    result = 31 * result + targetReplication;
    return result;
  }
}
