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

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.LightWeightCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ensuring the namenode retry cache works correctly for
 * non-idempotent requests.
 * 
 * Retry cache works based on tracking previously received request based on the
 * ClientId and CallId received in RPC requests and storing the response. The
 * response is replayed on retry when the same request is received again.
 * 
 * The test works by manipulating the Rpc {@link Server} current RPC call. For
 * testing retried requests, an Rpc callId is generated only once using
 * {@link #newCall()} and reused for many method calls. For testing non-retried
 * request, a new callId is generated using {@link #newCall()}.
 */
public class TestNamenodeRetryCache {
  private static final byte[] CLIENT_ID = ClientId.getClientId();
  private static MiniDFSCluster cluster;
  private static FSNamesystem namesystem;
  private static PermissionStatus perm = new PermissionStatus(
      "TestNamenodeRetryCache", null, FsPermission.getDefault());
  private static DistributedFileSystem filesystem;
  private static int callId = 100;
  private static Configuration conf = new HdfsConfiguration();
  private static final int BlockSize = 512;
  private static final int expirityTime = 30000;
  
  /** Start a cluster */
  @Before
  public void setup() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY, 30000);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    filesystem = cluster.getFileSystem();
  }
  
  /** Cleanup after the test 
   * @throws IOException 
   * @throws UnresolvedLinkException 
   * @throws SafeModeException 
   * @throws AccessControlException */
  @After
  public void cleanup() throws IOException {
    namesystem.multiTransactionalDelete("/", true);
  }
  
  public static void incrementCallId() {
    callId++;
  }
  
  /** Set the current Server RPC call */
  public static void newCall() {
    Server.Call call = new Server.Call(++callId, 1, null, null,
        RpcKind.RPC_PROTOCOL_BUFFER, CLIENT_ID);
    Server.getCurCall().set(call);
  }
  
  public static void resetCall() {
    Server.Call call = new Server.Call(RpcConstants.INVALID_CALL_ID, 1, null,
        null, RpcKind.RPC_PROTOCOL_BUFFER, RpcConstants.DUMMY_CLIENT_ID);
    Server.getCurCall().set(call);
  }
  
  private void concatSetup(String file1, String file2) throws Exception {
    DFSTestUtil.createFile(filesystem, new Path(file1), BlockSize, (short)1, 0L);
    DFSTestUtil.createFile(filesystem, new Path(file2), BlockSize, (short)1, 0L);
  }
  
  /**
   * Tests for concat call
   */
  @Test
  public void testConcat() throws Exception {
    resetCall();
    String file1 = "/testNamenodeRetryCache/testConcat/file1";
    String file2 = "/testNamenodeRetryCache/testConcat/file2";
    
    // Two retried concat calls succeed
    concatSetup(file1, file2);
    newCall();
    namesystem.concat(file1, new String[]{file2});
    namesystem.concat(file1, new String[]{file2});
    namesystem.concat(file1, new String[]{file2});
    
    // A non-retried concat request fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      namesystem.concat(file1, new String[]{file2});
      Assert.fail("testConcat - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
  
  /**
   * Tests for delete call
   */
  @Test
  public void testDelete() throws Exception {
    String dir = "/testNamenodeRetryCache/testDelete";
    // Two retried calls to create a non existent file
    newCall();
    namesystem.mkdirs(dir, perm, true);
    newCall();
    Assert.assertTrue(namesystem.multiTransactionalDelete(dir, false));
    Assert.assertTrue(namesystem.multiTransactionalDelete(dir, false));
    Assert.assertTrue(namesystem.multiTransactionalDelete(dir, false));
    
    // non-retried call fails and gets false as return
    newCall();
    Assert.assertFalse(namesystem.multiTransactionalDelete(dir, false));
  }
  
  /**
   * Test for createSymlink
   */
  @Test
  public void testCreateSymlink() throws Exception {
    String target = "/testNamenodeRetryCache/testCreateSymlink/target";
    
    // Two retried symlink calls succeed
    newCall();
    namesystem.createSymlink(target, "/a/b", perm, true);
    namesystem.createSymlink(target, "/a/b", perm, true);
    namesystem.createSymlink(target, "/a/b", perm, true);
    
    // non-retried call fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      namesystem.createSymlink(target, "/a/b", perm, true);
      Assert.fail("testCreateSymlink - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
  
  /**
   * Test for create file
   */
  @Test
  public void testCreate() throws Exception {
    String src = "/testNamenodeRetryCache/testCreate/file";
    // Two retried calls succeed
    newCall();
    HdfsFileStatus status = namesystem.startFile(src, perm, "holder",
        "clientmachine", EnumSet.of(CreateFlag.CREATE), true, (short) 1, BlockSize);
    Assert.assertEquals(status.getFileId(), namesystem.startFile(src, perm, 
        "holder", "clientmachine", EnumSet.of(CreateFlag.CREATE), 
        true, (short) 1, BlockSize).getFileId());
    Assert.assertEquals(status.getFileId(), namesystem.startFile(src, perm, 
        "holder", "clientmachine", EnumSet.of(CreateFlag.CREATE), 
        true, (short) 1, BlockSize).getFileId());
    
    // A non-retried call fails
    newCall();
    try {
      namesystem.startFile(src, perm, "holder", "clientmachine",
          EnumSet.of(CreateFlag.CREATE), true, (short) 1, BlockSize);
      Assert.fail("testCreate - expected exception is not thrown");
    } catch (IOException e) {
      // expected
    }
  }
  
  /**
   * Test for rename1
   */
  @Test
  public void testAppend() throws Exception {
    String src = "/testNamenodeRetryCache/testAppend/src";
    resetCall();
    // Create a file with partial block
    DFSTestUtil.createFile(filesystem, new Path(src), 128, (short)1, 0L);
    
    // Retried append requests succeed
    newCall();
    LocatedBlock b = namesystem.appendFile(src, "holder", "clientMachine");
    Assert.assertEquals(b.getBlock().getBlockId(), namesystem.appendFile(src, "holder", "clientMachine").getBlock().
        getBlockId());
    Assert.assertEquals(b.getBlock().getBlockId(), namesystem.appendFile(src, "holder", "clientMachine").getBlock().
        getBlockId());
    
    // non-retried call fails
    newCall();
    try {
      namesystem.appendFile(src, "holder", "clientMachine");
      Assert.fail("testAppend - expected exception is not thrown");
    } catch (Exception e) {
      // Expected
    }
  }
  
  /**
   * Test for rename1
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testRename1() throws Exception {
    String src = "/testNamenodeRetryCache/testRename1/src";
    String target = "/testNamenodeRetryCache/testRename1/target";
    resetCall();
    namesystem.mkdirs(src, perm, true);
    
    // Retried renames succeed
    newCall();
    Assert.assertTrue(namesystem.multiTransactionalRename(src, target));
    Assert.assertTrue(namesystem.multiTransactionalRename(src, target));
    Assert.assertTrue(namesystem.multiTransactionalRename(src, target));
    
    // A non-retried request fails
    newCall();
    Assert.assertFalse(namesystem.multiTransactionalRename(src, target));
  }
  
  /**
   * Test for rename2
   */
  @Test
  public void testRename2() throws Exception {
    String src = "/testNamenodeRetryCache/testRename2/src";
    String target = "/testNamenodeRetryCache/testRename2/target";
    resetCall();
    namesystem.mkdirs(src, perm, true);
    
    // Retried renames succeed
    newCall();
    namesystem.multiTransactionalRename(src, target, Rename.NONE);
    namesystem.multiTransactionalRename(src, target, Rename.NONE);
    namesystem.multiTransactionalRename(src, target, Rename.NONE);
    
    // A non-retried request fails
    newCall();
    try {
      namesystem.multiTransactionalRename(src, target, Rename.NONE);
      Assert.fail("testRename 2 expected exception is not thrown");
    } catch (IOException e) {
      // expected
    }
  }
  
  
  @Test
  public void testRetryCacheConfig() {
    // By default retry configuration should be enabled
    Configuration conf = new HdfsConfiguration();
    Assert.assertNotNull(FSNamesystem.initRetryCache(conf)); 
    
    // If retry cache is disabled, it should not be created
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, false);
    Assert.assertNull(FSNamesystem.initRetryCache(conf));
  }
  
  /**
   * After run a set of operations, restart NN and check if the retry cache has
   * been rebuilt based on the editlog.
   */
  @Test
  public void testRetryCacheRebuild() throws Exception {
    DFSTestUtil.runOperations(cluster, filesystem, conf, BlockSize, 0);
    
    LightWeightCache<CacheEntry, CacheEntry> cacheSet = 
        (LightWeightCache<CacheEntry, CacheEntry>) namesystem.getRetryCache().getCacheSet();
    assertEquals(11, cacheSet.size());
    
    Map<CacheEntry, CacheEntry> oldEntries = 
        new HashMap<CacheEntry, CacheEntry>();
    Iterator<CacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // restart NameNode
    cluster.restartNameNode();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    
    // check retry cache
    assertTrue(namesystem.hasRetryCache());
    fillCacheFromDB(oldEntries);
    cacheSet = (LightWeightCache<CacheEntry, CacheEntry>) namesystem
        .getRetryCache().getCacheSet();
    assertEquals(11, cacheSet.size());
    iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      assertTrue(oldEntries.containsKey(entry));
    }
  }
  
  private void fillCacheFromDB(Map<CacheEntry, CacheEntry> oldEntries) throws IOException {
    for (final CacheEntry entry : oldEntries.keySet()) {
      HopsTransactionalRequestHandler rh = new HopsTransactionalRequestHandler(HDFSOperationType.CONCAT) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          locks.add(lf.getRetryCacheEntryLock(entry.getClientId(), entry.getCallId()));
        }

        @Override
        public Object performTask() throws IOException {
          namesystem.getRetryCache().getCacheSet().get(entry);
          return null;
        }
      };
      rh.handle();

    }
  }
  
  /**
   * After run a set of operations, restart NN and check if the retry cache has
   * been rebuilt based on the editlog.
   */
  @Test
  public void testRetryCacheCleaning() throws Exception {
    DFSTestUtil.runOperations(cluster, filesystem, conf, BlockSize, 0);
    
    LightWeightCache<CacheEntry, CacheEntry> cacheSet = 
        (LightWeightCache<CacheEntry, CacheEntry>) namesystem.getRetryCache().getCacheSet();
    assertEquals(11, cacheSet.size());
    
    Map<CacheEntry, CacheEntry> oldEntries = 
        new HashMap<CacheEntry, CacheEntry>();
    Iterator<CacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // restart NameNode
    cluster.restartNameNode();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    
    Thread.sleep(expirityTime*3);
    
    // check retry cache
    assertTrue(namesystem.hasRetryCache());
    fillCacheFromDB(oldEntries);
    cacheSet = (LightWeightCache<CacheEntry, CacheEntry>) namesystem
        .getRetryCache().getCacheSet();
    assertEquals(0, cacheSet.size());
  }
  
}