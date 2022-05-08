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

import io.hops.common.INodeResolver;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RetriableException;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;

public class INodeLock extends BaseINodeLock {
  
  private final TransactionLockTypes.INodeLockType lockType;
  private final TransactionLockTypes.INodeResolveType resolveType;
  private boolean resolveLink;
  protected final String[] paths;
  protected final long inodeId;
  private Collection<Long> ignoredSTOInodes;
  protected boolean skipReadingQuotaAttr;
  protected long namenodeId;
  protected Collection<ActiveNode> activeNamenodes;

  INodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String... paths) {
    super();
    this.lockType = lockType;
    this.resolveType = resolveType;
    this.resolveLink = false;
    this.activeNamenodes = null;
    this.ignoredSTOInodes = new ArrayList<>();
    this.namenodeId = -1;
    this.paths = paths;
    this.inodeId = -1;
    this.skipReadingQuotaAttr = false;
  }

  INodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, long inodeId) {
    super();
    this.lockType = lockType;
    this.resolveType = resolveType;
    this.resolveLink = false;
    this.activeNamenodes = null;
    this.ignoredSTOInodes = new ArrayList<>();
    this.namenodeId = -1;
    this.paths = null;
    this.inodeId = inodeId;
    this.skipReadingQuotaAttr = false;
  }
    
  public INodeLock setIgnoredSTOInodes(long inodeID) {
    this.ignoredSTOInodes.add(inodeID);
    return this;
  }

  public INodeLock setNameNodeID(long nnID) {
    this.namenodeId = nnID;
    return this;
  }

  public INodeLock setActiveNameNodes(Collection<ActiveNode> activeNamenodes){
    this.activeNamenodes = activeNamenodes;
    return this;
  }

  public INodeLock skipReadingQuotaAttr(boolean val){
    this.skipReadingQuotaAttr = val;
    return this;
  }

  public INodeLock resolveSymLink(boolean resolveLink) {
    this.resolveLink = resolveLink;
    return this;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (paths != null) {
      /*
       * Needs to be sorted in order to avoid deadlocks. Otherwise one transaction
       * could acquire path0 and path1 in the given order while another one does
       * it in the opposite order, more precisely path1, path0, what could cause
       * a dealock situation.
       */
      Arrays.sort(paths);
      if (LOG.isTraceEnabled()) LOG.trace("Acquiring INode locks on the following paths: " + StringUtils.join(paths, ", "));
      acquirePathsINodeLocks();
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Acquiring INode lock on INode with ID=" + inodeId);
      acquireInodeIdInodeLock();
    }
    if (!skipReadingQuotaAttr) {
      acquireINodeAttributes();
    }
  }

  protected void acquireInodeIdInodeLock() throws IOException {
    if (!resolveType.equals(TransactionLockTypes.INodeResolveType.PATH) && !resolveType.equals(
        TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) && !resolveType.equals(
            TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY)) {
      throw new IllegalArgumentException("Unknown type " + resolveType.name());
    }
    List<INode> resolvedINodes = resolveUsingCache(lockType, inodeId);

    String path = INodeUtil.constructPath(resolvedINodes);
    addPathINodesAndUpdateResolvingCache(path, resolvedINodes);

    if (resolvedINodes!=null && resolvedINodes.size() > 0) {
      INode lastINode = resolvedINodes.get(resolvedINodes.size() - 1);
      if (resolveType == TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
        List<INode> children = findImmediateChildren(lastINode);
        addChildINodes(path, children);
      } else if (resolveType == TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY) {
        List<INode> children = findChildrenRecursively(lastINode);
        addChildINodes(path, children);
      }
    }
  }

  protected void acquirePathsINodeLocks() throws IOException {
    if (!resolveType.equals(TransactionLockTypes.INodeResolveType.PATH) &&
            !resolveType.equals(
                    TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) &&
            !resolveType.equals(
                    TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY)) {
      throw new IllegalArgumentException("Unknown type " + resolveType.name());
    }

    if (LOG.isTraceEnabled()) LOG.trace("Acquiring locks on " + paths.length + " path(s). Lock type: " + lockType.name() +
            ", resolve type: " + resolveType.name());

    for (String path : paths) {
      if (LOG.isTraceEnabled()) LOG.trace("Attempting to acquire " + lockType.name() + " lock for path: " + path + "");
      List<INode> resolvedINodes = null;
      if (getDefaultInodeLockType() == TransactionLockTypes.INodeLockType.READ_COMMITTED) {
        if (LOG.isTraceEnabled()) LOG.trace("Attempting to resolve path '" + path + "' using INode Hint Cache.");
        //Batching only works in READ_COMITTED mode. If locking is enabled then it can lead to deadlocks.
        resolvedINodes = resolveUsingCache(path);
      }

      if (resolvedINodes == null) {
        if (LOG.isTraceEnabled()) LOG.trace("Path '" + path + "' was either not in INode Hint Cache or we couldn't use the cache.");
        // path not found in the cache
        // set random partition key if enabled
        if (setRandomParitionKeyEnabled) {
          setPartitioningKey(rand.nextLong());
        }
        resolvedINodes = acquireINodeLockByPath(path);
        addPathINodesAndUpdateResolvingCache(path, resolvedINodes);
      }

      if (resolvedINodes.size() > 0) {
        INode lastINode = resolvedINodes.get(resolvedINodes.size() - 1);
        if (resolveType ==
                TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
          List<INode> children = findImmediateChildren(lastINode);
          addChildINodes(path, children);
        } else if (resolveType ==
                TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY) {
          List<INode> children = findChildrenRecursively(lastINode);
          addChildINodes(path, children);
        }
      }
    }
  }

  private List<INode> resolveUsingCache(String path) throws IOException {
    CacheResolver cacheResolver = getCacheResolver();
    if (cacheResolver == null) {
      return null;
    }
    List<INode> resolvedINodes = cacheResolver.fetchINodes(lockType, path, resolveLink);
    if (resolvedINodes != null) {
      if (LOG.isTraceEnabled()) LOG.trace("Resolved " + resolvedINodes.size() + " INode(s) via INode Hint Cache.");
      for (INode iNode : resolvedINodes) {
        if(iNode!=null){
          checkSubtreeLock(iNode);
        }
      }
      handleLockUpgrade(resolvedINodes, INode.getPathComponents(path), path);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Failed to resolve any INodes via INode Hint Cache.");
    }
    return resolvedINodes;
  }

  private List<INode> acquireINodeLockByPath(String path)
          throws IOException {
    List<INode> resolvedINodes = new ArrayList<>();
    byte[][] components = INode.getPathComponents(path);

    INode currentINode;
    if (isRootTarget(components)) {
      resolvedINodes.add(acquireLockOnRoot(lockType));
      return resolvedINodes;
    } else if (isRootParent(components) &&
        TransactionLockTypes.impliesParentWriteLock(this.lockType)) {
      currentINode = acquireLockOnRoot(lockType);
    } else {
      currentINode = acquireLockOnRoot(getDefaultInodeLockType());
    }
    resolvedINodes.add(currentINode);

    INodeResolver resolver =
        new INodeResolver(components, currentINode, resolveLink, true);
    while (resolver.hasNext()) {
      TransactionLockTypes.INodeLockType currentINodeLock =
          identifyLockType(lockType, resolver.getCount() + 1, components);
      setINodeLockType(currentINodeLock);
      if (LOG.isTraceEnabled()) {
        if (currentINode != null) LOG.trace("Current INode: " + currentINode.getLocalName() + " (id=" + currentINode.getId() + "). Resolving next component with lock " + currentINodeLock.name() + ".");
        else LOG.trace("Current INode: null. Resolving next component with lock " + currentINodeLock.name() + ".");
      }
      currentINode = resolver.next();

      if (currentINode != null) {
        addLockedINodes(currentINode, currentINodeLock);
        checkSubtreeLock(currentINode);
        resolvedINodes.add(currentINode);
      }
    }

    handleLockUpgrade(resolvedINodes, components, path);
    return resolvedINodes;
  }

  private boolean isRootTarget(byte[][] components) {
    return isTarget(0, components);
  }

  private boolean isRootParent(byte[][] components) {
    return isParent(0, components);
  }

  private void checkSubtreeLock(INode iNode) throws IOException {
    if(!iNode.isSTOLocked()){
      return;
    }

    boolean locked = false;
    // this check for active locks
    if (SubtreeLockHelper.isSTOLocked(iNode.isSTOLocked(), iNode.getSTOLockOwner(), activeNamenodes)) {
      locked = true;
      if (ignoredSTOInodes.contains(iNode.getId())) {
        // ignore this lock. this is needed for sub operations in a sub tree ops protocol
        locked = false;
      }
    } else { // the lock flag is set but the lock is dead
      // you can ignore the lock after some time. it is possible that
      // the NN is alive and its ID just changed because it is slow to HB
      long timePassed = System.currentTimeMillis() - getStoLockTime(iNode.getId());
      if (timePassed < NameNode.getFailedSTOCleanDelay()) {
        locked = true;
      } else {
        LOG.debug("Ignoring subtree lock as more than " + timePassed + " ms has passed.  Max " +
                "lock retention time is:" + NameNode.getFailedSTOCleanDelay());
      }
    }

    if (locked) {
      throw new RetriableException("The subtree " + iNode.getLocalName() + " is locked by " +
              "Namenode Id: " + iNode.getSTOLockOwner() + ". Active Namenodes are: " + activeNamenodes);
    }
  }

  private long getStoLockTime(long inodeId) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.SUBTREE_GET_LOCK_TIME) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.getLockTime(inodeId);
              }
            };

    return (long)subTreeLockChecker.handle();
  }

  private void handleLockUpgrade(List<INode> resolvedINodes,
      byte[][] components, String path)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    // TODO Handle the case that predecessing nodes get deleted before locking
    // lock upgrade if the path was not fully resolved
    if (resolvedINodes.size() != components.length) {
      if (LOG.isTraceEnabled()) LOG.trace("Path '" + path + "' was not fully resolved. Resolved " +
              resolvedINodes.size() + "/" + components.length + " path components.");
      // path was not fully resolved
      INode inodeToReread = null;
      if (lockType ==
          TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
        if (resolvedINodes.size() <= components.length - 2) {
          inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
        }
      } else if (lockType == TransactionLockTypes.INodeLockType.WRITE) {
        inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
      }

     if (inodeToReread != null) {
        long partitionIdOfINodeToBeReRead = INode.calculatePartitionId(inodeToReread.getParentId(), inodeToReread
        .getLocalName(), inodeToReread.myDepth());
        if (LOG.isTraceEnabled())
          LOG.trace("Re-reading INode " + inodeToReread.getLocalName() + " (id=" + inodeToReread.getId() +
                 ") with lock " + lockType.name() + ", partitionId=" + partitionIdOfINodeToBeReRead + ", parentId=" +
                 inodeToReread.getParentId() + ".");
        INode inode = find(lockType, inodeToReread.getLocalName(),
            inodeToReread.getParentId(), partitionIdOfINodeToBeReRead);
        if (inode != null) {
          // re-read after taking write lock to make sure that no one has created the same inode.
          addLockedINodes(inode, lockType);
          String existingPath = buildPath(path, resolvedINodes.size());
          if (LOG.isTraceEnabled())
            LOG.trace("Successfully re-read INode " + inode.getLocalName() + " (id=" + inode.getId() +
                    "). Existing path = '" + existingPath + "'. Acquiring " + lockType.name() +
                    " lock on rest of path '" + path + "'.");
          List<INode> rest =
              acquireLockOnRestOfPath(lockType, inode, path, existingPath,
                  false);
          resolvedINodes.addAll(rest);
        }
      }
    }  else if (LOG.isTraceEnabled()) {
      if (LOG.isTraceEnabled()) LOG.trace("Fully resolved '" + path + "'. Resolved " +
              resolvedINodes.size() + "/" + components.length + " path components.");
    }
  }

  private List<INode> acquireLockOnRestOfPath(
      TransactionLockTypes.INodeLockType lock, INode baseInode, String fullPath,
      String prefix, boolean resolveLink)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    List<INode> resolved = new ArrayList<>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    INodeResolver resolver =
        new INodeResolver(fullComps, baseInode, resolveLink, true,
            prefixComps.length - 1);
    while (resolver.hasNext()) {
      setINodeLockType(lock);
      INode current = resolver.next();
      if (current != null) {
        addLockedINodes(current, lock);
        resolved.add(current);
      }
    }
    return resolved;
  }

  private List<INode> findImmediateChildren(INode lastINode)
      throws StorageException, TransactionContextException {
    List<INode> children = new ArrayList<>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        setINodeLockType(TransactionLockTypes.INodeLockType.READ_COMMITTED); //if the parent is locked then taking lock on all children is not necessary
        children.addAll(((INodeDirectory) lastINode).getChildrenList());
      }
    }
    return children;
  }

  private List<INode> findChildrenRecursively(INode lastINode)
      throws StorageException, TransactionContextException {
    LinkedList<INode> children = new LinkedList<>();
    LinkedList<INode> unCheckedDirs = new LinkedList<>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        unCheckedDirs.add(lastINode);
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        setINodeLockType(TransactionLockTypes.INodeLockType.READ_COMMITTED); //locking the parent is sufficient
        List<INode> clist = ((INodeDirectory) next).getChildrenList();
        unCheckedDirs.addAll(clist);
        children.addAll(clist);
      }
    }
    LOG.trace("Added " + children.size() + " children.");
    return children;
  }

  private INode acquireLockOnRoot(TransactionLockTypes.INodeLockType lock)
      throws StorageException, TransactionContextException {
    LOG.trace("Acquiring " + lock + " on the root node");
    return find(lock, INodeDirectory.ROOT_NAME, HdfsConstantsClient.GRANDFATHER_INODE_ID, INodeDirectory.
        getRootDirPartitionKey());
  }

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);

    for (int i = 0; i < Math.min(components.length, size); i++) {
      if (i == 0) {
        builder.append("/");
      } else {
        if (i != 1) {
          builder.append("/");
        }
        builder.append(DFSUtil.bytes2String(components[i]));
      }
    }

    return builder.toString();
  }
  
  protected INode find(String name, long parentId, long partitionId)
      throws StorageException, TransactionContextException {
    return find(lockType, name, parentId, partitionId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("INodeLock {");
    if(paths != null && paths.length > 0){
      sb.append("paths=");
      sb.append(Arrays.toString(paths));
      sb.append(", ");
    }

    if ( inodeId != -1){
      sb.append("INodeID: ");
      sb.append(inodeId);
      sb.append(",");
    }

    if(lockType != null){
      sb.append("lockType=");
      sb.append(lockType);
      sb.append(" ");
    }

    sb.append("}");
    return sb.toString();
  }
}
