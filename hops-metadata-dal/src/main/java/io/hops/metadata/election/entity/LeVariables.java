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
package io.hops.metadata.election.entity;

import java.util.StringTokenizer;

public class LeVariables {

  private boolean evictFlag;
  private long timePeriod;
  private long maxId;
  private final static String EVICT_FLAG = "EVICT_FLAG";
  private final static String TIME_PERIOD = "TIME_PERIOD";
  private final static String MAX_ID = "MAX_ID";

  public LeVariables(boolean evictFlag, long time_period, long max_id) {
    this.evictFlag = evictFlag;
    this.timePeriod = time_period;
    this.maxId = max_id;
  }

  public static LeVariables parseString(String val) {
    StringTokenizer st = new StringTokenizer(val, ";");

    assert st.countTokens() == 3;

    String variable = st.nextToken();
    StringTokenizer st2 = new StringTokenizer(variable, ":");
    assert st2.countTokens() == 2;
    st2.nextToken();
    boolean evictFlag = Boolean.parseBoolean(st2.nextToken());


    variable = st.nextToken();
    st2 = new StringTokenizer(variable, ":");
    assert st2.countTokens() == 2;
    st2.nextToken();
    long time_period = Long.parseLong(st2.nextToken());


    variable = st.nextToken();
    st2 = new StringTokenizer(variable, ":");
    assert st2.countTokens() == 2;
    st2.nextToken();
    long max_id = Long.parseLong(st2.nextToken());

    return new LeVariables(evictFlag, time_period, max_id);
  }

  @Override
  public String toString() {
    return EVICT_FLAG + ":" + evictFlag + ";" + TIME_PERIOD + ":" + timePeriod +
        ";" + MAX_ID + ":" + maxId;
  }

  public boolean isEvictFlag() {
    return evictFlag;
  }

  public long getTimePeriod() {
    return timePeriod;
  }

  public long getMaxId() {
    return maxId;
  }

  public void setEvictFlag(boolean evictFlag) {
    this.evictFlag = evictFlag;
  }

  public void setTimePeriod(long timePeriod) {
    this.timePeriod = timePeriod;
  }

  public void setMaxId(long maxId) {
    this.maxId = maxId;
  }

}