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
package io.hops.metadata.yarn.entity.quota;

import java.util.HashSet;
import java.util.Set;

public class ProjectDailyCost {

  private String projectName;
  private String projectUser;
  private long day;
  private float creditsUsed;
  private Set<String> appIds = new HashSet<>();
  
  public ProjectDailyCost(String projectName, String projectUser, long day,
          float creditsUsed, String appIds) {
    this.projectName = projectName;
    this.projectUser = projectUser;
    this.day = day;
    this.creditsUsed = creditsUsed;
    for(String appId:appIds.split(",")){
      if(!appId.isEmpty()){
        this.appIds.add(appId);
      }
    }
  }

  @Override
  public String toString() {
    return "YarnProjectsQuota{" + "projectName=" + projectName + ", user="
            + projectUser + ", day=" + day + ", credit=" + creditsUsed + " }";
  }
  
  public void incrementCharge(float inc, String appId){
    creditsUsed+=inc;
    appIds.add(appId);
  }

  /**
   * @return the projectName
   */
  public String getProjectName() {
    return projectName;
  }

  /**
   * @return the projectUser
   */
  public String getProjectUser() {
    return projectUser;
  }

  /**
   * @return the day
   */
  public long getDay() {
    return day;
  }

  /**
   * @return the creditsUsed
   */
  public float getCreditsUsed() {
    return creditsUsed;
  }

  /**
   * @param projectName the projectName to set
   */
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  /**
   * @param projectUser the projectUser to set
   */
  public void setProjectUser(String projectUser) {
    this.projectUser = projectUser;
  }

  /**
   * @param day the day to set
   */
  public void setDay(long day) {
    this.day = day;
  }

  /**
   * @param creditsUsed the creditsUsed to set
   */
  public void setCreditsUsed(float creditsUsed) {
    this.creditsUsed = creditsUsed;
  }

    public Set<String> getAppIds() {
        return appIds;
    }

}
