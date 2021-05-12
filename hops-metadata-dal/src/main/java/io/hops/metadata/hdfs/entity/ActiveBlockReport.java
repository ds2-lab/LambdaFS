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

public class ActiveBlockReport {
  private String dnAddress;
  private long nnId;
  private String nnAddress;
  private long startTime;
  private long numBlocks;

  public ActiveBlockReport(String dnAddress, long nnId, String nnAddress,
                           long startTime, long numBlocks) {
    this.dnAddress = dnAddress;
    this.nnId = nnId;
    this.nnAddress = nnAddress;
    this.startTime = startTime;
    this.numBlocks = numBlocks;
  }

  public String getDnAddress() {
    return dnAddress;
  }

  public long getNnId() {
    return nnId;
  }

  public String getNnAddress(){
    return nnAddress;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getNumBlocks() {
    return numBlocks;
  }

  public void setDnId(String dnAddress) {
    this.dnAddress = dnAddress;
  }

  public void setNnId(long nnId) {
    this.nnId = nnId;
  }

  public void setNNAddress(String nnAddress){
    this.nnAddress = nnAddress;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setNumBlocks(long numBlocks) {
    this.numBlocks = numBlocks;
  }
}
