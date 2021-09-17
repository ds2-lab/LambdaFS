/*
 * Copyright (C) 2019 hops.io.
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
package io.hops.metadata.yarn.entity;

import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;

public class AppProvenanceEntry {

  private final String id;
  private final String name;
  private String state;
  private final String user;
  private final long timestamp;
  private final long submitTime;
  private final long startTime;
  private final long finishTime;

  public AppProvenanceEntry(String id, String name, String state, String user, long timestamp, 
    long submitTime, long startTime, long finishTime) {
    this.id = id;
    this.name = name;
    this.state = state;
    this.user = user;
    this.timestamp = timestamp;
    this.submitTime = submitTime;
    this.startTime = startTime;
    this.finishTime = finishTime;
  }
  
  public AppProvenanceEntry(ApplicationState appState, long timestamp, 
    long submitTime, long startTime, long finishTime) {
    this(appState.getApplicationId(), appState.getName(), appState.getState(), appState.getUser(), 
      timestamp, submitTime, startTime, finishTime);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getState() {
    return state;
  }

  public String getUser() {
    return user;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  @Override
  public String toString() {
    return "AppProvenanceEntry{" + "id=" + id + ", name=" + name + ", state=" + state + ", user=" + user 
      + ", timestamp=" + timestamp + ", submitTime=" + submitTime + ", startTime=" + startTime 
      + ", finishTime=" + finishTime + '}';
  }
  
  public void setState(String state) {
    this.state = state;
  }
}
