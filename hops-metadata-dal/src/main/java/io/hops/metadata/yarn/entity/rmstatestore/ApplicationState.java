
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
package io.hops.metadata.yarn.entity.rmstatestore;

public class ApplicationState {

  private final String applicationId;
  private final byte[] appstate;
  //for hopsworks
  private final String user;
  private final String name;
  private final String state;

  public ApplicationState(String applicationid) {
    this(applicationid, null, null, null, null);
  }
  
  public ApplicationState(String applicationId, byte[] appstate, String user,
      String name, String state) {
    this.applicationId = applicationId;
    this.appstate = appstate;
    this.name = name;
    this.user = user;
    this.state = state;
  }

  public String getApplicationid() {
    return this.applicationId;
  }

  public byte[] getAppstate() {
    return this.appstate;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getUser() {
    return user;
  }

  public String getName() {
    return name;
  }

  public String getState() {
    return state;
  }
  

  @Override
  public String toString() {
    String str = "HopApplicationState{" + "applicationid=" + applicationId;
    if (appstate != null) {
      str += ", appstate length=" + appstate.length;
    }
    str += '}';
    return str;
  }
}
