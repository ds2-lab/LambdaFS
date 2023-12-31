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

import io.hops.common.IDsMonitor;
import io.hops.exception.StorageException;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.security.GroupAlreadyExistsException;
import io.hops.security.UserAlreadyInGroupException;
import io.hops.security.UsersGroups;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFsLimits {
  static Configuration conf;
  static FSNamesystem fs;
  static boolean fsIsReady;

//  Usersg
  static PermissionStatus perms =
      new PermissionStatus("admin", "admin", FsPermission.getDefault());

  
  static private FSNamesystem getMockNamesystem() throws IOException {
    ServerlessNameNode nn = mock(ServerlessNameNode.class);
    when(nn.getActiveNameNodes())
        .thenReturn(new SortedActiveNodeListPBImpl(Collections.EMPTY_LIST));
    FSNamesystem fsn = new FSNamesystem(conf,nn, -1);
    fsn.setImageLoaded(fsIsReady);
    //needed to create the root inode
    FSDirectory fsd = new FSDirectory(fsn, conf);
    return fsn;
  }
  
  private void initFS() throws StorageException, IOException {
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());
    ServerlessNameNode.format(conf);
    try {
      UsersGroups.addUser(perms.getUserName());
    } catch (UserAlreadyInGroupException e){}
    try {
      UsersGroups.addGroup(perms.getGroupName());
    } catch (GroupAlreadyExistsException e){}
    try {
      UsersGroups.addUserToGroup(perms.getUserName(), perms.getGroupName());
    } catch(UserAlreadyInGroupException e){}
    IDsMonitor.getInstance().start();
    ServerlessNameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    fs = null;
    fsIsReady = true;
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_KEY,
            false);
    initFS();
  }

  @Test
  public void testNoLimits() throws Exception {
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
    mkdirs("/55555", null);
  }

  @Test
  public void testMaxComponentLength() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", PathComponentTooLongException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testMaxComponentLengthRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    mkdirs("/5", null);
    rename("/5", "/555", PathComponentTooLongException.class);
    rename("/5", "/55", null);
    mkdirs("/6", null);
    deprecatedRename("/6", "/666", PathComponentTooLongException.class);
    deprecatedRename("/6", "/66", null);
  }

  @Test
  public void testMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", MaxDirectoryItemsExceededException.class);
  }
  
    @Test
  public void testMaxDirItemsRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/2", null);
    mkdirs("/2/A", null);
    rename("/2/A", "/A", MaxDirectoryItemsExceededException.class);
    rename("/2/A", "/1/A", null);
    mkdirs("/2/B", null);
    deprecatedRename("/2/B", "/B", MaxDirectoryItemsExceededException.class);
    deprecatedRename("/2/B", "/1/B", null);
    rename("/1", "/3", null);
    deprecatedRename("/2", "/4", null);
  }
  
  @Test
  public void testMaxDirItemsLimits() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 0);
    try {
      mkdirs("/1", null);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot set dfs", e);
    }
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 64*100*1024);
    try {
      mkdirs("/1", null);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot set dfs", e);
    }
  }

  @Test
  public void testMaxComponentsAndMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testDuringEditLogs() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    fsIsReady = false;
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
  }

  private static long id = 1 + INodeDirectory.ROOT_INODE_ID;

  
  @Test
  /**
   * This test verifies that error string contains the
   * right parent directory name if the operation fails with
   * PathComponentTooLongException
   */
  public void testParentDirectoryNameIsCorrect() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 20);
    mkdirs("/user", null);
    mkdirs("/user/testHome", null);
    mkdirs("/user/testHome/FileNameLength", null);

    mkdirCheckParentDirectory(
      "/user/testHome/FileNameLength/really_big_name_0003_fail",
      "/user/testHome/FileNameLength", PathComponentTooLongException.class);

    renameCheckParentDirectory("/user/testHome/FileNameLength",
      "/user/testHome/really_big_name_0003_fail", "/user/testHome/",
      PathComponentTooLongException.class);

  }


  /**
   * Verifies that Parent Directory is correct after a failed call to mkdir
   * @param name Directory Name
   * @param ParentDirName Expected Parent Directory
   * @param expected Exception that is expected
   * @throws Exception
   */
  private void mkdirCheckParentDirectory(String name, String ParentDirName,
                                         Class<?> expected)
    throws Exception {
    verify(mkdirs(name, expected), ParentDirName);
  }

  /**
   *
   /**
   * Verifies that Parent Directory is correct after a failed call to mkdir
   * @param name Directory Name
   * @param dst Destination Name
   * @param ParentDirName Expected Parent Directory
   * @param expected Exception that is expected
   * @throws Exception
   */
  private void renameCheckParentDirectory(String name, String dst,
                                          String ParentDirName,
                                          Class<?> expected)
    throws Exception {
    verify(rename(name, dst, expected), ParentDirName);
  }

  /**
   * verifies the ParentDirectory Name is present in the message given.
   * @param message - Expection Message
   * @param ParentDirName - Parent Directory Name to look for.
   */
  private void verify(String message, String ParentDirName) {
    boolean found = false;
    if (message != null) {
      String[] tokens = message.split("\\s+");
      for (String token : tokens) {
        if (token != null && token.equals(ParentDirName)) {
          found = true;
          break;
        }
      }
    }
    assertTrue(found);
  }

  private String mkdirs(String name, Class<?> expected)
  throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    String errorString = null;
    try {
      fs.mkdirs(name, perms, false);
    } catch (Throwable e) {
      generated = e.getClass();
      e.printStackTrace();
      errorString = e.getMessage();
    }
    assertEquals(expected, generated);
    return errorString;
  }

  private String rename(String src, String dst, Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    String errorString = null;
    try {
      Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;
      fs.renameTo(src, dst, new Rename[]{});
    } catch (Throwable e) {
      generated = e.getClass();
      errorString = e.getMessage();
    }
    assertEquals(expected, generated);
    return errorString;
  }

  @SuppressWarnings("deprecation")
  private void deprecatedRename(final String src, final String dst, final Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    try {
      Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;
      fs.renameTo(src, dst, new Rename[]{});
    } catch (Throwable e) {
      generated = e.getClass();
    }
    assertEquals(expected, generated);
  }

  private static void lazyInitFSDirectory() throws IOException {
    // have to create after the caller has had a chance to set conf values
    if (fs == null) {
      fs = getMockNamesystem();
    }
  }
}
