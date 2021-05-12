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
package io.hops.transaction.lock;

public class TransactionLockTypes {
  public enum LockType {
    /**
     * No lock
     */
    READ_COMMITTED,
    /**
     * Read lock
     */
    READ,
    /**
     * Write lock
     */
    WRITE
  }

  public enum LeaseHolderResolveType {
    /**
     * All Paths
     */
    ALL_PATHS,
    /**
     * Single Path
     */
    SINGLE_PATH
  }

  public enum INodeLockType {
    /**
     * No lock
     */
    READ_COMMITTED,
    /**
     * Read lock
     */
    READ,
    /**
     * Write lock
     */
    WRITE,
    /**
     * Write lock on the target component and its parent.
     */
    WRITE_ON_TARGET_AND_PARENT
  }

  public enum INodeResolveType {
    /**
     * Resolve only the given path
     */
    PATH,
    /**
     * Resolve path and find the given directory's children
     */
    PATH_AND_IMMEDIATE_CHILDREN,
    /**
     * Resolve the given path and find all the children recursively.
     */
    PATH_AND_ALL_CHILDREN_RECURSIVELY
  }

  public static boolean impliesTargetWriteLock(INodeLockType lockType) {
    switch (lockType) {
      case WRITE:
      case WRITE_ON_TARGET_AND_PARENT:
        return true;
      default:
        return false;
    }
  }

  public static boolean impliesTargetReadLock(INodeLockType lockType) {
    switch (lockType) {
      case READ:
        return true;
      default:
        return false;
    }
  }

  public static boolean impliesParentWriteLock(INodeLockType lockType) {
    switch (lockType) {
      case WRITE_ON_TARGET_AND_PARENT:
        return true;
      default:
        return false;
    }
  }
}
