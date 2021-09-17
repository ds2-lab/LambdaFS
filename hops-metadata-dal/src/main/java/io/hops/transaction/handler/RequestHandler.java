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
package io.hops.transaction.handler;

import io.hops.StorageConnector;
import io.hops.log.NDCWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Random;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class RequestHandler {
  private long waitTime;

  public interface OperationType {
  }

  protected static Log requestHandlerLOG = LogFactory.getLog(RequestHandler.class);
  protected Object[] params = null;
  // TODO These should be in a config file
  protected static int RETRY_COUNT = 5;
  protected static int BASE_WAIT_TIME = 2000;
  protected OperationType opType;
  protected List<String> subOpNames = new ArrayList<>();
  protected static StorageConnector connector;
  protected static Random rand = new Random(System.currentTimeMillis());

  public static void setStorageConnector(StorageConnector c) {
    connector = c;
  }

  public static void setRetryCount(final int retryCount){
    RETRY_COUNT = retryCount;
    requestHandlerLOG.debug("Transaction Retry Count is: "+RETRY_COUNT);
  }

  public static void setRetryBaseWaitTime(final int baseWaitTime){
    BASE_WAIT_TIME = baseWaitTime;
    requestHandlerLOG.debug("Trasaction wait time before retry is: "+BASE_WAIT_TIME);
  }

  public RequestHandler(OperationType opType) {
    this.opType = opType;
  }

  public Object handle() throws IOException {
    return handle(null);
  }

  public Object handle(Object info) throws IOException {
    waitTime = 0;
    return execute(info);
  }

  protected abstract Object execute(Object info) throws IOException;

  public abstract Object performTask() throws IOException;

  public RequestHandler setParams(Object... params) {
    this.params = params;
    return this;
  }

  public Object[] getParams() {
    return this.params;
  }

  protected long exponentialBackoff() {
    try {
      if (waitTime > 0) {
        if(requestHandlerLOG.isTraceEnabled()) {
          requestHandlerLOG.trace("TX is being retried. Waiting for " + waitTime +
                  " ms before retry. TX name " + opType);
        }
        Thread.sleep(waitTime);
      }
      if (waitTime == 0) {
        waitTime = rand.nextInt((int)BASE_WAIT_TIME);
      } else {
        waitTime = waitTime * 2;
      }
      return waitTime;
    } catch (InterruptedException ex) {
      requestHandlerLOG.warn(ex);
      Thread.currentThread().interrupt();
    }
    return 0;
  }

  protected void resetWaitTime() {
    waitTime = 0;
  }
}
