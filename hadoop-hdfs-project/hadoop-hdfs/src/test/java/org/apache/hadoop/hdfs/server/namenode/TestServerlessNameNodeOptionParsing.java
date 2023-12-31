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

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.junit.Assert;
import org.junit.Test;

public class TestServerlessNameNodeOptionParsing {

  @Test(timeout = 10000)
  public void testUpgrade() {
    StartupOption opt = null;
    // UPGRADE is set, but nothing else
    opt = ServerlessNameNode.parseArguments(new String[] {"-upgrade"});
    assertEquals(opt, StartupOption.UPGRADE);
    assertNull(opt.getClusterId());
    // cluster ID is set
    opt = ServerlessNameNode.parseArguments(new String[] { "-upgrade", "-clusterid",
        "mycid" });
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals("mycid", opt.getClusterId());
    
    opt = ServerlessNameNode.parseArguments(new String[] { "-upgrade", "-cid"});
    assertNull(opt);
  }

  @Test(timeout = 10000)
  public void testRollingUpgrade() {
    {
      final String[] args = {"-rollingUpgrade"};
      final StartupOption opt = ServerlessNameNode.parseArguments(args);
      assertNull(opt);
    }

    {
      final String[] args = {"-rollingUpgrade", "started"};
      final StartupOption opt = ServerlessNameNode.parseArguments(args);
      assertEquals(StartupOption.ROLLINGUPGRADE, opt);
      assertEquals(RollingUpgradeStartupOption.STARTED, opt.getRollingUpgradeStartupOption());
      assertTrue(RollingUpgradeStartupOption.STARTED.matches(opt));
    }

    {
      final String[] args = {"-rollingUpgrade", "downgrade"};
      final StartupOption opt = ServerlessNameNode.parseArguments(args);
      assertEquals(StartupOption.ROLLINGUPGRADE, opt);
      assertEquals(RollingUpgradeStartupOption.DOWNGRADE, opt.getRollingUpgradeStartupOption());
      assertTrue(RollingUpgradeStartupOption.DOWNGRADE.matches(opt));
    }

    {
      final String[] args = {"-rollingUpgrade", "rollback"};
      final StartupOption opt = ServerlessNameNode.parseArguments(args);
      assertEquals(StartupOption.ROLLINGUPGRADE, opt);
      assertEquals(RollingUpgradeStartupOption.ROLLBACK, opt.getRollingUpgradeStartupOption());
      assertTrue(RollingUpgradeStartupOption.ROLLBACK.matches(opt));
    }

    {
      final String[] args = {"-rollingUpgrade", "foo"};
      try {
        ServerlessNameNode.parseArguments(args);
        Assert.fail();
      } catch(IllegalArgumentException iae) {
        // the exception is expected.
      }
    }
  }
    
}
