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


public class ProjectDailyId {
    private final String projectName;
    private final String projectUser;
    private final long day;
    
    public ProjectDailyId(String projectName, String projectUser, long day) {
        this.projectName = projectName;
        this.projectUser = projectUser;
        this.day = day;
    }
    
    @Override
    public String toString() {
        return "YarnProjectsQuota{" + "projectName=" + projectName + ", user=" + projectUser  + ", day=" + day  + " }";
    }
    
    @Override
    public boolean equals(Object o) {
        if (o != null && getClass() == o.getClass()) {
          ProjectDailyId other = (ProjectDailyId) o;
          if (day == other.day){
              if (projectName.equals(other.projectName) 
                  && projectUser.equals(other.projectUser)) {
                  return true;
              }
          }
        }
        return false;  
    }
    
    @Override
    public int hashCode() {
      return projectName.hashCode() + projectUser.hashCode() + (int)day;
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

}
