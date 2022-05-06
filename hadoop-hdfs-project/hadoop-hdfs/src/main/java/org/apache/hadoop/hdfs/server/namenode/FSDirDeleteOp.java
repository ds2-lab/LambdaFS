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
package org.apache.hadoop.hdfs.server.namenode;

import com.ctc.wstx.util.StringUtil;
import com.google.gson.JsonObject;
import io.hops.metadata.hdfs.entity.*;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.serverless.BaseHandler;
import org.apache.hadoop.hdfs.serverless.invoking.ArgumentContainer;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessInvokerBase;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hadoop.util.Time.now;

class FSDirDeleteOp {
  public static final Log LOG = LogFactory.getLog(FSDirDeleteOp.class);
  
  public static long BIGGEST_DELETABLE_DIR;

  /**
   * When deleting the contents of a directory one-by-one, we will partition that operation across multiple
   * Serverless NameNodes to execute it in-parallel. We send each other Serverless NN this many paths to delete.
   */
  public static int SUBTREE_DELETE_BATCH_SIZE;

  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param fsd the FSDirectory instance
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param collectedBlocks Blocks under the deleted directory
   * @param removedINodes INodes that should be removed from inodeMap
   * @return the number of files that have been removed
   */
  static long delete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws IOException {
    if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
      ServerlessNameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    final long filesRemoved;
    if (!deleteAllowed(iip, iip.getPath()) ) {
      filesRemoved = -1;
    } else {
      filesRemoved = unprotectedDelete(fsd, iip, collectedBlocks,
                                       removedINodes, mtime);
    }

    LOG.debug("Removed " + filesRemoved + " file(s).");
    return filesRemoved;
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   *
   * @param fsn namespace
   * @param srcArg path name to be deleted
   * @param recursive boolean true to apply to all sub-directories recursively
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static boolean delete(
      final FSNamesystem fsn, String srcArg, final boolean recursive)
      throws IOException {
    LOG.debug("Performing DELETE operation on source argument " + srcArg + " now. Recursive: " + recursive + ".");
    final FSDirectory fsd = fsn.getFSDirectory();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(fsd.getPermissionChecker(), srcArg, pathComponents);

    if (!recursive) {
      LOG.debug("Deletion is NOT recursive. Performing delete transaction now.");
      // It is safe to do this as it will only delete a single file or an empty directory
      return deleteTransaction(fsn, src, recursive);
    }

    PathInformation pathInfo = fsn.getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INode pathInode = pathInfo.getINodesInPath().getLastINode();

    if (pathInode == null) {
      ServerlessNameNode.stateChangeLog
          .debug("Failed to remove " + src + " because it does not exist");
      return false;
    } else if (pathInode.isRoot()) {
      ServerlessNameNode.stateChangeLog.warn("Failed to remove " + src
          + " because the root is not allowed to be deleted");
      return false;
    }

    INodeIdentifier subtreeRoot = null;
    if (pathInode.isFile() || pathInode.isSymlink()) {
      boolean success = deleteTransaction(fsn, src, false);

      if (success)
        LOG.debug("DELETE was successful. Returning true.");
      else
        LOG.debug("DELETE was NOT successful. Returning false.");

      return success;
    }

    RetryCacheEntry cacheEntry = LightWeightCacheDistributed.getTransactional();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      LOG.debug("DELETE was successful (returning previous response).");
      return true; // Return previous response
    }
    boolean ret = false;
    try {
      //if quota is enabled then only the leader namenode can delete the directory.
      //this is because before the deletion is done the quota manager has to apply all the outstanding
      //quota updates for the directory. The current design of the quota manager is not distributed.
      //HopsFS clients send the delete operations to the leader namenode if quota is enabled
      if (!fsn.isLeader() && fsd.isQuotaEnabled()) {
        throw new QuotaUpdateException("Unable to delete the file " + src
            + " because Quota is enabled and I am not the leader");
      }

      //sub tree operation
      try {
        LOG.debug("Performing subtree operation to delete " + src + " now...");
        //once subtree is locked we still need to check all subAccess in AbstractFileTree.FileTree
        //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
        //access:null, subAccess:FsAction.ALL, ignoreEmptyDir:true
        subtreeRoot = fsn.lockSubtreeAndCheckOwnerAndParentPermission(src, false,
            FsAction.WRITE, SubTreeOperation.Type.DELETE_STO);

        // LOG.debug("Subtree root: " + subtreeRoot.toString());

        List<AclEntry> nearestDefaultsForSubtree = fsn.calculateNearestDefaultAclForSubtree(pathInfo);
        AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(fsn, subtreeRoot, FsAction.ALL, true,
            nearestDefaultsForSubtree, subtreeRoot.getStoragePolicy());
        long s = System.currentTimeMillis();
        fileTree.buildUp(fsd.getBlockStoragePolicySuite());
        long t = System.currentTimeMillis();
        LOG.debug("Built-up file tree for '" + srcArg + "' for DELETE operation in " + (t - s) + " ms.");
        fsn.delayAfterBbuildingTree("Built tree for " + srcArg + " for delete op");

        Set<Integer> associatedDeployments = fileTree.getAssociatedDeployments();
        boolean canProceed = ConsistencyProtocol.runConsistencyProtocolForSubtreeOperation(associatedDeployments, src);
        if (!canProceed) {
          LOG.debug("DELETE was NOT successful. Returning false.");
          return false;
        }

        if (fsd.isQuotaEnabled()) {
          Iterator<Long> idIterator = fileTree.getAllINodesIds().iterator();
          synchronized (idIterator) {
            fsn.getQuotaUpdateManager().addPrioritizedUpdates(idIterator);
            try {
              idIterator.wait();
            } catch (InterruptedException e) {
              // Not sure if this can happen if we are not shutting down, but we need to abort in case it happens.
              throw new IOException("Operation failed due to an Interrupt");
            }
          }
        }

        for (int i = fileTree.getHeight(); i > 0; i--) {
          if (!deleteTreeLevel(fsn, src, fileTree.getSubtreeRoot().getId(), fileTree, i)) {
            LOG.debug("DELETE was NOT successful. Returning false.");
            return false;
          }
        }
      } finally {
        if (subtreeRoot != null) {
          fsn.unlockSubtree(src, subtreeRoot.getInodeId());
        }
      }
      LOG.debug("DELETE was successful. Returning true.");
      return true;
    } finally {
      LightWeightCacheDistributed.putTransactional(ret);
    }
  }

//  /**
//   * Process a batch of paths to delete.
//   *
//   * @param paths The paths to delete.
//   * @param barrier Barrier containing Futures, where the Futures represent individual deletions.
//   * @param fsn Reference to the local FSNamesystem instance.
//   * @param subTreeRootID INode ID of the root of the subtree.
//   */
//  private static void deleteBatch(Collection<String> paths, List<Future> barrier, final FSNamesystem fsn,
//                                     final long subTreeRootID) {
//    for (final String path : paths) {
//      Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
//      barrier.add(f);
//    }
//  }

  private static boolean deleteTreeLevel(final FSNamesystem fsn, final String subtreeRootPath, final long subTreeRootID,
                                        final AbstractFileTree.FileTree fileTree, int level) throws IOException {
    if (LOG.isDebugEnabled()) LOG.debug("Deleting tree level " + level + " of tree rooted at " + subtreeRootPath + " (ID = " +subTreeRootID + ") now...");

    ArrayList<Future> barrier = new ArrayList<>();

    List<ProjectedINode> emptyDirs = new ArrayList();

    for (final ProjectedINode dir : fileTree.getDirsByLevel(level)) {
      int numChildren = fileTree.countChildren(dir.getId());
      if (LOG.isDebugEnabled()) LOG.debug("Children in directory (id=" + dir.getId() + "): " + numChildren);
      if (numChildren <= BIGGEST_DELETABLE_DIR) { // Can we delete the directory directly?
        if (LOG.isDebugEnabled()) LOG.debug("Directory " + dir.getId() + " has less than " + BIGGEST_DELETABLE_DIR + " children. Can delete it directly.");
        final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);

        Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
        barrier.add(f);
      }
      else { // Cannot delete directory. So, delete contents of directory one-by-one.
        // Delete the content of the directory one by one.
        LOG.debug("Directory " + dir.getId() + " has too many child files (" + numChildren +
                "). Deleting content of directory one-by-one.");

        Collection<ProjectedINode> children = fileTree.getChildren(dir.getId());

        ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);

        // If we cannot get the instance for some reason (shouldn't happen), or if there just aren't enough files
        // to batch across multiple NNs, then we'll perform the delete operations locally.
        if (instance == null || children.size() <= SUBTREE_DELETE_BATCH_SIZE) {
          // These if statements are just to determine what message to log.
          if (instance == null)
            LOG.error("Cannot retrieve singleton ServerlessNameNode instance for batched subtree delete.");
          else
            if (LOG.isDebugEnabled()) LOG.debug("There are not enough files to batch across multiple NNs (Performing all deletes locally.");

          for (final ProjectedINode inode : children) {
            if (LOG.isDebugEnabled()) LOG.debug("    Trying to delete child INode " + inode.getName() + " (id=" + inode.getId() + ").");
            if (!inode.isDirectory()) {
              final String path = fileTree.createAbsolutePath(subtreeRootPath, inode);
              Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
              barrier.add(f);
            }
          }
        }
        else {
          List<String[]> batches = new ArrayList<>();
          String[] currentBatch = new String[SUBTREE_DELETE_BATCH_SIZE];

          if (LOG.isDebugEnabled()) LOG.debug("Splitting the " + children.size() + " child files into " + children.size() / SUBTREE_DELETE_BATCH_SIZE + " batches of size ~" + SUBTREE_DELETE_BATCH_SIZE + ".");

          // Create batches of paths of size 'SUBTREE_DELETE_BATCH_SIZE'.
          // We will partition these batches across multiple other NameNodes in order to complete them in-parallel.
          // I do this manually rather than using the `partition` function from commons.collections or Guava so that
          // we can construct the paths for the various nodes as we go.
          int idx = 0;

          // TODO: Can we potentially optimize this by only iterating over children once?
          //       We basically do it twice. Once here when creating the batches, and again when deleting them.
          //       Can we generate the batches in O(1), and then do the extra processing during the O(n) delete
          //       step? By "extra processing", I mean convert each child to a full path. Just depends on if
          //       we can split up the `children` set, because it has type Collection<ProjectedINode>.
          for (ProjectedINode node : children) {
            if (node.isDirectory()) continue; // Skip directories like we do when performing the deletes locally.

            final String path = fileTree.createAbsolutePath(subtreeRootPath, node);
            currentBatch[idx++] = path;

            if (idx >= SUBTREE_DELETE_BATCH_SIZE) {
              batches.add(currentBatch);
              currentBatch = new String[SUBTREE_DELETE_BATCH_SIZE];
              idx = 0;
            }
          }

          ArrayList<Future> remoteBarrier = new ArrayList<>();
          // It's possible that we could end up with just one batch (or maybe none) if the directory contains
          // a bunch of directories, rather than actual files. So, let's make sure we have multiple batches
          // to process before trying to offload batches to other NNs.
          if (batches.size() > 1 && !BaseHandler.localModeEnabled) {
            ExecutorService executorService = Executors.newFixedThreadPool(batches.size() - 1);
            ServerlessInvokerBase<JsonObject> serverlessInvoker = instance.getServerlessInvoker();
            // final HashMap<String, Future<Boolean>> futures = new HashMap<>();
            final String serverlessEndpointBase = instance.getServerlessEndpointBase();
            if (LOG.isDebugEnabled()) LOG.debug("Submitting " + (batches.size() - 1) + " batch(es) of deletes to other NameNodes.");
            for (int i = 1; i < batches.size(); i++) {
              String[] batch = batches.get(i);
              String requestId = UUID.randomUUID().toString();

              ArgumentContainer argumentContainer = new ArgumentContainer();
              argumentContainer.addPrimitive("subtreeRootId", subTreeRootID);
              argumentContainer.addPrimitive("leaderNameNodeID", instance.getId());
              argumentContainer.addNonByteArray("paths", batch);

              int targetDeployment = -1;

              // Randomly pick a deployment different from our own so that we don't invoke ourselves, as that
              // would defeat the purpose of offloading/batching these delete operations to another NameNode.
              while (targetDeployment == -1 || targetDeployment == instance.getDeploymentNumber()) {
                targetDeployment = ThreadLocalRandom.current().nextInt(0, instance.getNumDeployments());
              }

              if (LOG.isDebugEnabled()) LOG.debug("Targeting deployment " + targetDeployment + " for batch " + i + "/" + batches.size());

              int finalTargetDeployment = targetDeployment;
              Future<Boolean> future = executorService.submit(() -> {
                JsonObject response = serverlessInvoker.invokeNameNodeViaHttpPost(
                        "subtreeDeleteSubOperation", serverlessEndpointBase, new HashMap<>(),
                        argumentContainer, requestId, finalTargetDeployment);

                // Attempt to extract the result. If it is null, then return false. Otherwise, return the result.
                Object result = serverlessInvoker.extractResultFromJsonResponse(response);
                if (result == null) return false;
                return (boolean) result;
              });

              // futures.put(requestId, future);
              remoteBarrier.add(future);
            }
          }
          else if (BaseHandler.localModeEnabled) { // Just enables us to write a different print than the base case.
            LOG.warn("LocalMode is enabled. We cannot offload to other NNs. Must complete operation locally.");
          }
          else { // Base case. Just one batch. We'll process it locally.
            LOG.warn("We somehow ended up with just one batch. No need to offload deletes to other NNs.");
          }

          // If "Local Mode" is enabled, then we'll process all the batches locally, one after another.
          // If "Local Mode" is disabled, then we only want to process the 0-th batch locally. The other
          // batches will have been off-loaded to other NameNodes, and thus we do not execute them here.
          int lastBatchIndexExclusive = (BaseHandler.localModeEnabled ? batches.size() : 1);

          // Add all the futures corresponding to NameNode invocations to the barrier. There could 0 or more.
          barrier.addAll(remoteBarrier);

          // It is theoretically possible that we end up with zero batches. This could occur if the directory
          // we're processing exclusively contains other directories and no files. So, let's make sure we
          // have at least one batch before trying to process a batch locally.
          for (int i = 0; i < lastBatchIndexExclusive; i++) {
            String[] localBatch = batches.get(i);
            // Process the local batch of deletes.
            if (LOG.isDebugEnabled()) LOG.debug("Processing local batch " + (i+1) + "/" + batches.size() + " now.");
            for (String path : localBatch) {
              Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
              barrier.add(f);
            }
          }
        }

        emptyDirs.add(dir);
      }
    }

    if (LOG.isDebugEnabled()) LOG.debug("There are " + barrier.size() + " child files to delete next.");

    boolean success = processResponses(barrier);
    if (!success)
      return false;

    if (LOG.isDebugEnabled()) LOG.debug("There are " + emptyDirs.size() + " empty child directories to delete next.");

    //delete the empty Dirs
    for (ProjectedINode dir : emptyDirs) {
      final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
      Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
      barrier.add(f);
    }

    return processResponses(barrier);
  }

  public static boolean processResponses(ArrayList<Future> barrier) throws IOException {
    boolean result = true;
    for (Future f : barrier) {
      try {
        if (!((Boolean) f.get())) {
          result = false;
        }
      } catch (ExecutionException e) {
        result = false;
        LOG.error("Exception was thrown during partial delete", e);
        Throwable throwable = e.getCause();
        if (throwable instanceof IOException) {
          throw (IOException) throwable; //pass the exception as is to the client
        } else {
          throw new IOException(e); //only io exception as passed to clients.
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  // Made public so that we can call it from the ServerlessNameNode class when off-loading/batching these operations.
  public static Future multiTransactionDeleteInternal(final FSNamesystem fsn, final String src, final long subTreeRootId) {
    final FSDirectory fsd = fsn.getFSDirectory();

    LOG.debug("Performing multi-transaction internal delete for " + src + " now...");

    return fsn.getFSOperationsExecutor().submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // IMPORTANT: We are passing 'true' for the 'skipConsistencyProtocol' parameter here, as this function
        // only gets executed in subtree operations. If we've gotten this far, then the consistency protocol already
        // executed successfully at the beginning of this subtree op, and therefore we do not need to run it again.
        HopsTransactionalRequestHandler deleteHandler = new HopsTransactionalRequestHandler(
            HDFSOperationType.SUBTREE_DELETE, null, true) {
          @Override
          public void acquireLock(TransactionLocks locks) {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, src)
                .setNameNodeID(fsn.getNamenodeId())
                .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!fsd.isQuotaEnabled())
                .setIgnoredSTOInodes(subTreeRootId);
            locks.add(il).add(lf.getLeaseLockAllPaths(LockType.WRITE,
                    fsn.getLeaseCreationLockRows()))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                .add(lf.getBlockLock()).add(
                lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE, BLK.IV, BLK.ER));

            locks.add(lf.getAllUsedHashBucketsLock());

            if (fsd.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src));
            }

            if (fsn.isErasureCodingEnabled()) {
              locks.add(lf.getEncodingStatusLock(true, LockType.WRITE, src));
            }
            locks.add(lf.getEZLock(LockType.WRITE));
          }

          @Override
          public Object performTask() throws IOException {
            final INodesInPath iip = fsd.getINodesInPath4Write(src);
            if (!deleteInternal(fsn, src, iip)) {
              // at this point the delete op is expected to succeed. Apart from DB errors
              // this can only fail if the "quiesce phase" in the subtree operation failed to
              // quiesce the subtree. See TestSubtreeConflicts.testConcurrentSTOandInodeOps
              throw new RetriableException("Unable to Delete path: " + src + "." + " Possible subtree quiesce failure");
            }
            return true;
          }
        };
        return (Boolean) deleteHandler.handle(this);
      }
    });
  }
    
  static boolean deleteTransaction(
      final FSNamesystem fsn, String srcArg, final boolean recursive)
      throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);

    LOG.debug("Performing delete transaction for source " + src + " now...");

    HopsTransactionalRequestHandler deleteHandler = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src)
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il).add(lf.getLeaseLockAllPaths(LockType.WRITE, fsn.getLeaseCreationLockRows()))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR,BLK.PE, BLK.IV,BLK.ER));
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId(), Server.getRpcEpoch()));
        }

        locks.add(lf.getAllUsedHashBucketsLock());

        if (fsd.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src));
        }
        if (fsn.isErasureCodingEnabled()) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
        }
        locks.add(lf.getEZLock(LockType.WRITE));
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        RetryCacheEntry cacheEntry = LightWeightCacheDistributed.get();
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return true; // Return previous response
        }
        boolean ret = false;
        try {

          final INodesInPath iip = fsd.getINodesInPath4Write(src, false);
          if (!recursive && fsd.isNonEmptyDirectory(iip)) {
            throw new PathIsNotEmptyDirectoryException(src + " is non empty");
          }
          if (fsd.isPermissionEnabled()) {
            fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                FsAction.ALL, true);
          }
          ret = deleteInternal(fsn, src, iip);
          return ret; 
        } finally {
          LightWeightCacheDistributed.put(null, ret);
        }
      }
    };
    return (Boolean) deleteHandler.handle();
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   * @param fsn namespace
   * @param src path name to be deleted
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static boolean deleteInternal(FSNamesystem fsn, String src, INodesInPath iip) throws IOException {
    if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
      ServerlessNameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    
    long mtime = now();

    // Unlink the target directory from directory tree
    long filesRemoved = delete(fsd, iip, collectedBlocks, removedINodes, mtime);
    if (filesRemoved < 0)
      return false;

    incrDeletedFileCount(filesRemoved);

    if (LOG.isDebugEnabled()) LOG.debug("Removed INodes: " + StringUtils.join(", ", removedINodes));
    fsn.removeLeasesAndINodes(src, removedINodes);
    fsn.removeBlocks(collectedBlocks); // Incremental deletion of blocks
    collectedBlocks.clear();

    if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
      ServerlessNameNode.stateChangeLog.debug("DIR* Namesystem.delete: " + src +" is removed");
    }
    return true;
  }

  static void incrDeletedFileCount(long count) {
    ServerlessNameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }

  private static boolean deleteAllowed(final INodesInPath iip,
      final String src) {
    if (iip.length() < 1 || iip.getLastINode() == null) {
      if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
        ServerlessNameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: failed to remove "
                + src + " because it does not exist");
      }
      return false;
    } else if (iip.length() == 1) { // src is the root
      ServerlessNameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedDelete: failed to remove " + src +
              " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param fsd the FSDirectory instance
   * @param iip the inodes resolved from the path
   * @param collectedBlocks blocks collected from the deleted path
   * @param removedINodes inodes that should be removed from inodeMap
   * @param mtime the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */
  private static long unprotectedDelete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws IOException {

    // check if target node exists
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return -1;
    }
    
    // check if target node is the root
    if (iip.length() == 1) {
      return -1;
    }
  
    // Add metadata log entry for all deleted childred.
    addMetaDataLogForDirDeletion(targetNode, fsd.getFSNamesystem().getNamenodeId());

    // Remove the node from the namespace
    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return -1;
    }

    // set the parent's modification time
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime);
    if (removed == 0) {
      return 0;
    }    
            
    // collect block
    targetNode.destroyAndCollectBlocks(fsd.getBlockStoragePolicySuite(),
        collectedBlocks, removedINodes);
    
    if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
      ServerlessNameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return removed;
  }
  
  private static void addMetaDataLogForDirDeletion(INode targetNode, long namenodeId) throws IOException {
    if (targetNode.isDirectory()) {
      List<INode> children = ((INodeDirectory) targetNode).getChildrenList();
      for(INode child : children){
       if(child.isDirectory()){
         addMetaDataLogForDirDeletion(child, namenodeId);
       }else{
         child.logMetadataEvent(INodeMetadataLogEntry.Operation.Delete);
         child.logProvenanceEvent(namenodeId, FileProvenanceEntry.Operation.delete());
       }
      }
    }
    targetNode.logMetadataEvent(INodeMetadataLogEntry.Operation.Delete);
    targetNode.logProvenanceEvent(namenodeId, FileProvenanceEntry.Operation.delete());
  }
}
