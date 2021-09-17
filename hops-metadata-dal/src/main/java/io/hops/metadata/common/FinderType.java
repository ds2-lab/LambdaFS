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
package io.hops.metadata.common;

public interface FinderType<T> {
  public static enum Annotation {
    PrimaryKey,
    PrunedIndexScan,
    IndexScan,
    Batched,
    BatchedPrunedIndexScan,
    FullTableScan,
    FullTable,
    CacheOnly;

    public String getShort() {
      switch (this) {
        case PrimaryKey:
          return "PK";
        case PrunedIndexScan:
          return "PI";
        case IndexScan:
          return "IS";
        case Batched:
          return "B";
        case BatchedPrunedIndexScan:
          return "BPI";
        case FullTableScan:
          return "FTS";
        case FullTable:
          return "FT";
        case CacheOnly:
          return "CO";
        default:
          throw new IllegalStateException();
      }
    }
  }

  public Class getType();

  public Annotation getAnnotated();
}
