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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * Test NameNode port defaulting code.
 */
public class TestDefaultServerlessNameNodePort {

  @Test
  public void testGetAddressFromString() throws Exception {
    assertEquals(ServerlessNameNode.getAddress("foo").getPort(), ServerlessNameNode.DEFAULT_PORT);
    assertEquals(ServerlessNameNode.getAddress("hdfs://foo/").getPort(),
        ServerlessNameNode.DEFAULT_PORT);
    assertEquals(ServerlessNameNode.getAddress("hdfs://foo:555").getPort(), 555);
    assertEquals(ServerlessNameNode.getAddress("foo:555").getPort(), 555);
  }

  @Test
  public void testGetAddressFromConf() throws Exception {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://foo/");
    assertEquals(ServerlessNameNode.getAddress(conf).getPort(), ServerlessNameNode.DEFAULT_PORT);
    FileSystem.setDefaultUri(conf, "hdfs://foo:555/");
    assertEquals(ServerlessNameNode.getAddress(conf).getPort(), 555);
    FileSystem.setDefaultUri(conf, "foo");
    assertEquals(ServerlessNameNode.getAddress(conf).getPort(), ServerlessNameNode.DEFAULT_PORT);
  }

  @Test
  public void testGetUri() {
    assertEquals(ServerlessNameNode.getUri(new InetSocketAddress("foo", 555)),
        URI.create("hdfs://foo:555"));
    assertEquals(
        ServerlessNameNode.getUri(new InetSocketAddress("foo", ServerlessNameNode.DEFAULT_PORT)),
        URI.create("hdfs://foo"));
  }
}
