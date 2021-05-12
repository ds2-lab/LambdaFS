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


public class MisReplicatedRange {
  final long nnId;
  final long startIndex;

  public MisReplicatedRange(long nnId, long startIndex) {
    this.nnId = nnId;
    this.startIndex = startIndex;
  }

  public long getNnId() {
    return nnId;
  }

  public long getStartIndex() {
    return startIndex;
  }
  
}
