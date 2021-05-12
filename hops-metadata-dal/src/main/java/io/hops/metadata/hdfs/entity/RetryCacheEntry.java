/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.hdfs.entity;

import io.hops.metadata.common.FinderType;

import java.util.Arrays;
import java.util.Objects;


public class RetryCacheEntry {

  public static byte SUCCESS = (byte) 1;
  public static byte FAILED = (byte) -1;

   public enum Finder implements FinderType<RetryCacheEntry> {
     ByPK;

    @Override
    public Class getType() {
      return RetryCacheEntry.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this){
        case ByPK:
          return Annotation.PrimaryKey;
        default:
          throw new IllegalStateException();
      }
    }
  }

  private byte[] clientId;
  private int callId;
  private byte[] payload;
  private long expirationTime;
  private long epoch;
  private byte state;

  public RetryCacheEntry(byte[] clientId, int callId, long epoch) {
    this.clientId = clientId;
    this.callId = callId;
    this.payload = null;
    this.expirationTime = 0;
    this.state = 0;
    this.epoch = epoch;
  }

  public RetryCacheEntry(byte[] clientId, int callId, byte[] payload, long expirationTime,
                         long epoch, byte state) {
    this.clientId = clientId;
    this.callId = callId;
    this.payload = payload;
    this.expirationTime = expirationTime;
    this.epoch = epoch;
    this.state = state;
  }

  public byte[] getClientId() {
    return clientId;
  }

  public int getCallId() {
    return callId;
  }

  public byte[] getPayload() {
    return payload;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public void setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public byte getState() {
    return state;
  }

  public static class PrimaryKey {
    private byte[] clientId;
    private int callId;
    private long epoch;

    public PrimaryKey(byte[] clientId, int callId, long epoch) {
      this.clientId = clientId;
      this.callId = callId;
      this.epoch = epoch;
    }

    public byte[] getClientId() {
      return clientId;
    }

    public int getCallId() {
      return callId;
    }

    public long getEpoch() {
      return epoch;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PrimaryKey) {
        PrimaryKey otherPK = (PrimaryKey) o;
        return (clientId == otherPK.getClientId() &&
                callId == otherPK.getCallId() &&
                epoch == otherPK.epoch);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(clientId) +
              Integer.hashCode(callId) +
              Long.hashCode(epoch);
    }
  }

  @Override
  public String toString() {
    return "RetryCacheEntry{" +
            "clientId=" + Arrays.toString(clientId) +
            ", callId=" + callId +
            ", payload=" + Arrays.toString(payload) +
            ", expirationTime=" + expirationTime +
            ", epoch=" + epoch +
            ", state=" + state +
            "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RetryCacheEntry that = (RetryCacheEntry) o;
    return callId == that.callId &&
            epoch == that.epoch &&
            Arrays.equals(clientId, that.clientId);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(callId, epoch);
    result = 31 * result + Arrays.hashCode(clientId);
    return result;
  }

  public boolean isSuccess() {
    return getState() == SUCCESS;
  }
}
