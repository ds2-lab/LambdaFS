/*
 * Copyright (C) 2020 hops.io.
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

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultipleINodesLock extends BaseINodeLock {

  private final List<INodeIdentifier> inodeIdentifiers;
  private final TransactionLockTypes.INodeLockType lockType;
  private long[] inodeIds;

  public MultipleINodesLock(List<INodeIdentifier> inodeIdentifiers,
                            TransactionLockTypes.INodeLockType lockType) {
    this.inodeIdentifiers = inodeIdentifiers;
    this.inodeIds = new long[inodeIdentifiers.size()];
    this.lockType = lockType;
    if (lockType == TransactionLockTypes.INodeLockType.READ_COMMITTED) {
      throw new IllegalArgumentException("For READ_COMMITTED locks BatchedINodeLocks as better " +
              "performance");
    }
  }

  @Override
  public void acquire(TransactionLocks locks) throws IOException {
    if (inodeIdentifiers != null && !inodeIdentifiers.isEmpty()) {

      List<INode> inodes = orderedReadWithLock();

      for (INode inode : inodes) {
        if (inode != null) {
          List<INode> pathInodes = readUpInodes(inode);
          addPathINodesAndUpdateResolvingAndInMemoryCaches(INodeUtil.constructPath(pathInodes),
                  pathInodes);
        }
      }
      addIndividualINodes(inodes);
    } else {
      throw new StorageException(
              "INodeIdentifier object is not properly initialized ");
    }
  }

  private List<INode> orderedReadWithLock() throws TransactionContextException, StorageException {
    List<INode> inodes = new ArrayList();
    Collections.sort(inodeIdentifiers);
    for (int i = 0; i < inodeIdentifiers.size(); i++) {
      INodeIdentifier inodeIdentifier = inodeIdentifiers.get(i);
      inodeIds[i] = inodeIdentifier.getInodeId().longValue();
      INode inode = find(lockType,
              inodeIdentifier.getName(),
              inodeIdentifier.getPid(),
              inodeIdentifier.getPartitionId(),
              inodeIdentifier.getInodeId());
      if (inode != null) {
        inodes.add(inode);
      }
    }
    return inodes;
  }


  long[] getINodeIds() {
    return inodeIds;
  }

  @Override
  public String toString() {
    if (inodeIdentifiers != null) {
      return "Multiple INode Lock = { " + inodeIdentifiers.size() + " inodes locked " + " }";
    }
    return "No inodes selected for locking";
  }
}
