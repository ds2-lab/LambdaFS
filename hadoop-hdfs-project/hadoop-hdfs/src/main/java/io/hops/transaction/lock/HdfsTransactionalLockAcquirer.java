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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.invoking.ServerlessNameNodeClient;

import java.io.IOException;

public final class HdfsTransactionalLockAcquirer extends TransactionLockAcquirer {
  public static final Log LOG = LogFactory.getLog(HdfsTransactionalLockAcquirer.class);

  private final HdfsTransactionLocks locks;

  public HdfsTransactionalLockAcquirer() {
    locks = new HdfsTransactionLocks();
  }

  @Override
  public void acquire() throws IOException {
    for (Lock lock : locks.getSortedLocks()) {
      LOG.debug("Acquiring lock of type " + lock.getClass().getSimpleName() + " now...");
      lock.acquire(locks);
      LOG.debug("Acquired lock of type " + lock.getClass().getSimpleName() + " now...");
    }
  }

  @Override
  public TransactionLocks getLocks() {
    return locks;
  }
}
