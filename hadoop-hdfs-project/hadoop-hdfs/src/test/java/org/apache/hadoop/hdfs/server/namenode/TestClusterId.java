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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestClusterId {
  private static final Log LOG = LogFactory.getLog(TestClusterId.class);
  File hdfsDir;
  Configuration config;

  private String getClusterId(Configuration config) throws IOException {
    // see if cluster id not empty.
    String cid = StorageInfo.getStorageInfoFromDB().getClusterID();
    LOG.info("successfully formated : cid=" + cid);
    return cid;
  }

  @Before
  public void setUp() throws IOException {
    ExitUtil.disableSystemExit();

    String baseDir = PathUtils.getTestDirName(getClass());

    hdfsDir = new File(baseDir, "dfs/name");
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException(
          "Could not delete test directory '" + hdfsDir + "'");
    }
    LOG.info("hdfsdir is " + hdfsDir.getAbsolutePath());

    // as some tests might change these values we reset them to defaults before
    // every test
    StartupOption.FORMAT.setForceFormat(false);
    StartupOption.FORMAT.setInteractiveFormat(true);
    
    config = new Configuration();
  }

  @After
  public void tearDown() throws IOException {
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException(
          "Could not tearDown test directory '" + hdfsDir + "'");
    }
  }

  @Test(timeout = 300000)
  public void testFormatClusterIdOption() throws IOException {
    
    // 1. should format without cluster id
    ServerlessNameNode.format(config);
    // see if cluster id not empty.
    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));

    // 2. successful format with given clusterid
    StartupOption.FORMAT.setClusterId("mycluster");
    ServerlessNameNode.format(config);
    // see if cluster id matches with given clusterid.
    cid = getClusterId(config);
    assertTrue("ClusterId didn't match", cid.equals("mycluster"));

    // 3. format without any clusterid again. It should generate new
    //clusterid.
    StartupOption.FORMAT.setClusterId("");
    ServerlessNameNode.format(config);
    String newCid = getClusterId(config);
    assertFalse("ClusterId should not be the same", newCid.equals(cid));
  }

  /**
   * Test namenode format with -format option. Format should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 300000)
  public void testFormat() throws IOException {
    String[] argv = {"-format"};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }

  /**
   * Test namenode format with -format option when an empty name directory
   * exists. Format should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 300000)
  public void testFormatWithEmptyDir() throws IOException {

    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = {"-format"};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }

  /**
   * Test namenode format with -format -force options when name directory
   * exists. Format should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 450000)
  public void testFormatWithForce() throws IOException {

    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = {"-format", "-force"};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }

  /**
   * Test namenode format with -format -force -clusterid option when name
   * directory exists. Format should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 450000)
  public void testFormatWithForceAndClusterId() throws IOException {

    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String myId = "testFormatWithForceAndClusterId";
    String[] argv = {"-format", "-force", "-clusterid", myId};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cId = getClusterId(config);
    assertEquals("ClusterIds do not match", myId, cId);
  }

  /**
   * Test namenode format with -clusterid -force option. Format command should
   * fail as no cluster id was provided.
   *
   * @throws IOException
   */
  @Test(timeout = 450000)
  public void testFormatWithInvalidClusterIdOption() throws IOException {

    String[] argv = {"-format", "-clusterid", "-force"};
    PrintStream origErr = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream stdErr = new PrintStream(baos);
    System.setErr(stdErr);

    ServerlessNameNode.createNameNode(argv, config);

    // Check if usage is printed
    assertTrue(baos.toString("UTF-8").contains("Usage: java NameNode"));
    System.setErr(origErr);

    // check if the version file does not exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertFalse("Check version should not exist", version.exists());
  }

  /**
   * Test namenode format with -format -clusterid options. Format should fail
   * was no clusterid was sent.
   *
   * @throws IOException
   */
  @Test(timeout = 300000)
  public void testFormatWithNoClusterIdOption() throws IOException {

    String[] argv = {"-format", "-clusterid"};
    PrintStream origErr = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream stdErr = new PrintStream(baos);
    System.setErr(stdErr);

    ServerlessNameNode.createNameNode(argv, config);

    // Check if usage is printed
    assertTrue(baos.toString("UTF-8").contains("Usage: java NameNode"));
    System.setErr(origErr);

    // check if the version file does not exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertFalse("Check version should not exist", version.exists());
  }

  /**
   * Test namenode format with -format -clusterid and empty clusterid. Format
   * should fail as no valid if was provided.
   *
   * @throws IOException
   */
  @Test(timeout = 300000)
  public void testFormatWithEmptyClusterIdOption() throws IOException {

    String[] argv = {"-format", "-clusterid", ""};

    PrintStream origErr = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream stdErr = new PrintStream(baos);
    System.setErr(stdErr);

    ServerlessNameNode.createNameNode(argv, config);

    // Check if usage is printed
    assertTrue(baos.toString("UTF-8").contains("Usage: java NameNode"));
    System.setErr(origErr);

    // check if the version file does not exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertFalse("Check version should not exist", version.exists());
  }

  /**
   * Test namenode format with -format -nonInteractive options when name
   * directory does not exist. Format should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 300000)
  public void testFormatWithNonInteractiveNameDirDoesNotExit()
      throws IOException {

    String[] argv = {"-format", "-nonInteractive"};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }

  /**
   * Test namenode format with -force -nonInteractive -force option. Format
   * should succeed.
   *
   * @throws IOException
   */
  @Test(timeout = 450000)
  public void testFormatWithNonInteractiveAndForce() throws IOException {

    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = {"-format", "-nonInteractive", "-force"};
    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }

  /**
   * Test namenode format with -format option when a non empty name directory
   * exists. Enter Y when prompted and the format should succeed.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout = 300000)
  public void testFormatWithoutForceEnterYes()
      throws IOException, InterruptedException {

    // we check for a non empty dir, so create a child path
    File data = new File(hdfsDir, "file");
    if (!data.mkdirs()) {
      fail("Failed to create dir " + data.getPath());
    }

    // capture the input stream
    InputStream origIn = System.in;
    ByteArrayInputStream bins = new ByteArrayInputStream("Y\n".getBytes());
    System.setIn(bins);

    String[] argv = {"-format"};

    try {
      ServerlessNameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    System.setIn(origIn);

    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")));
  }
}