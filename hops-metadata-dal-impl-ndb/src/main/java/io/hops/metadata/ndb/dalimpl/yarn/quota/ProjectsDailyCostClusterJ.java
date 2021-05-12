/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.yarn.quota;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;

import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.entity.quota.ProjectDailyCost;
import io.hops.metadata.yarn.entity.quota.ProjectDailyId;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectsDailyCostClusterJ implements
        TablesDef.ProjectsDailyCostTableDef,
        ProjectsDailyCostDataAccess<ProjectDailyCost> {


  @PersistenceCapable(table = TABLE_NAME)
  public interface ProjectDailyCostDTO {

    @PrimaryKey
    @Column(name = PROJECTNAME)
    String getProjectName();

    void setProjectName(String projectName);

    @PrimaryKey
    @Column(name = USER)
    String getUser();

    void setUser(String user);

    @PrimaryKey
    @Column(name = DAY)
    long getDay();

    void setDay(long day);

    @Column(name = CREDITS_USED)
    float getCreditUsed();

    void setCreditUsed(float credit);
    
    @Column(name = APP_IDS)
    String getAppIds();

    void setAppIds(String appIds);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override 
  public ProjectDailyCost get(String projectName, String user, long day) throws StorageException{
    HopsSession session = connector.obtainSession();
    Object[] keys = new Object[]{projectName, user, day};
    ProjectDailyCostDTO dto = session.find(ProjectDailyCostDTO.class, keys);
    ProjectDailyCost result = null;
    if (dto != null) {
      result= createProjectDailyCost(dto);
    }
    session.release(dto);
    return result;
  }
  
  @Override
  public void add(ProjectDailyCost projectDailyCost) throws StorageException {
    HopsSession session = connector.obtainSession();
    ProjectDailyCostDTO dto = createPersistable(projectDailyCost, session);
      
    session.savePersistent(dto);
    session.release(dto);
  }
  
  @Override
  public Map<ProjectDailyId, ProjectDailyCost> getAll() throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ProjectDailyCostDTO> dobj = qb.
            createQueryDefinition(ProjectDailyCostDTO.class);
    HopsQuery<ProjectDailyCostDTO> query = session.createQuery(dobj);

    List<ProjectDailyCostDTO> queryResults = query.getResultList();
    Map<ProjectDailyId, ProjectDailyCost> result = createMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<ProjectDailyId, ProjectDailyCost> getByDay(long day)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType dobj = qb.createQueryDefinition(
            ProjectDailyCostDTO.class);

    HopsPredicate predicate = dobj.get(DAY).equal(dobj.param(DAY));
    dobj.where(predicate);
    HopsQuery query = session.createQuery(dobj);
    query.setParameter(DAY, day);

    List<ProjectDailyCostDTO> dtos = query.getResultList();
    Map<ProjectDailyId, ProjectDailyCost> result = createMap(dtos);
    session.release(dtos);
    return result;

  }

  public static Map<ProjectDailyId, ProjectDailyCost> createMap(
          List<ProjectDailyCostDTO> results) {
    Map<ProjectDailyId, ProjectDailyCost> map
            = new HashMap<>();
    for (ProjectDailyCostDTO persistable
            : results) {
      ProjectDailyCost hop = createProjectDailyCost(persistable);
      map.
              put(new ProjectDailyId(hop.getProjectName(), hop.
                              getProjectUser(), hop.getDay()), hop);
    }
    return map;
  }

  private static ProjectDailyCost createProjectDailyCost(
          ProjectDailyCostDTO csDTO) {
    ProjectDailyCost hop
            = new ProjectDailyCost(csDTO.getProjectName(), csDTO.getUser(),
                    csDTO.getDay(), csDTO.getCreditUsed(), csDTO.getAppIds());
    return hop;
  }

  @Override
  public void addAll(Collection<ProjectDailyCost> yarnProjectDailyCost)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProjectDailyCostDTO> toAdd
            = new ArrayList<>();
    for (ProjectDailyCost _yarnProjectDailyCost : yarnProjectDailyCost) {
      toAdd.add(createPersistable(_yarnProjectDailyCost, session));
    }
    session.savePersistentAll(toAdd);
    //    session.flush();
    session.release(toAdd);
  }

  private ProjectDailyCostDTO createPersistable(
          ProjectDailyCost hopPQ, HopsSession session) throws
          StorageException {
    ProjectDailyCostDTO pqDTO = session.
            newInstance(
                    ProjectDailyCostDTO.class);
    //Set values to persist new ContainerStatus
    pqDTO.setProjectName(hopPQ.getProjectName());
    pqDTO.setUser(hopPQ.getProjectUser());
    pqDTO.setDay(hopPQ.getDay());
    pqDTO.setCreditUsed(hopPQ.getCreditsUsed());
    String appIds = "";
    for(String appId: hopPQ.getAppIds()){
        appIds = appIds + appId + ",";
    }
    if(appIds.length()>3000){
        appIds = appIds.substring(0, 2997) +"...";
    }
    pqDTO.setAppIds(appIds);
    return pqDTO;
  }
}
