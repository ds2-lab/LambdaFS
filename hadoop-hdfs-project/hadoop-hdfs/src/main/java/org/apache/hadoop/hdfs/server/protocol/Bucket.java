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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;

public class Bucket {
  
  private BlockListAsLongs blocks;

  private byte[] hash;

  private boolean skip = false; // skip processing the bucket
  
  public Bucket(){}
  
  public Bucket(BlockListAsLongs blocks){
    this.blocks = blocks;
  }

  public void setBlocks(BlockListAsLongs blocks) {
    this.blocks = blocks;
  }
  
  public BlockListAsLongs getBlocks() {
    return blocks;
  }

  public void setHash(byte[] hash){
    this.hash = hash;
  }

  public byte[] getHash(){
    return hash;
  }

  public boolean isSkip() {
    return skip;
  }

  public void setSkip(boolean skip) {
    this.skip = skip;
  }
}