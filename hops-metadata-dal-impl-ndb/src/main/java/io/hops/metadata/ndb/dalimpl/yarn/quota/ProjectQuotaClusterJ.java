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
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.entity.quota.ProjectQuota;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectQuotaClusterJ implements TablesDef.ProjectQuotaTableDef,
        ProjectQuotaDataAccess<ProjectQuota> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ProjectQuotaDTO {

    @PrimaryKey
    @Column(name = PROJECT_NAME)
    String getProjectName();

    void setProjectName(String projectName);

    @Column(name = REMAINING_QUOTA)
    float getRemainingQuota();

    void setRemainingQuota(float credit);

    @Column(name = TOTAL_USED_QUOTA)
    float getTotalUsedQuota();

    void setTotalUsedQuota(float credit);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override 
  public ProjectQuota get(String projectName) throws StorageException{
    HopsSession session = connector.obtainSession();
    ProjectQuotaDTO dto = session.find(ProjectQuotaDTO.class, projectName);
    ProjectQuota result = null;
    if (dto != null) {
      result= createProjectQuota(dto);
    }
    session.release(dto);
    return result;
  }
  
  @Override
  public void add(ProjectQuota projectQuota) throws StorageException {
    HopsSession session = connector.obtainSession();
    
    ProjectQuotaDTO dto = createPersistable(projectQuota, session);
      
    session.savePersistent(dto);
    session.release(dto);
  }
  
  @Override
  public Map<String, ProjectQuota> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ProjectQuotaDTO> dobj = qb.createQueryDefinition(
            ProjectQuotaDTO.class);
    HopsQuery<ProjectQuotaDTO> query = session.createQuery(dobj);

    List<ProjectQuotaDTO> queryResults = query.getResultList();
    Map<String, ProjectQuota> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  public static Map<String, ProjectQuota> createMap(
          List<ProjectQuotaDTO> results) {
    Map<String, ProjectQuota> map = new HashMap<>();
    for (ProjectQuotaDTO persistable : results) {
      ProjectQuota hop = createProjectQuota(persistable);
      map.put(hop.getProjectid(), hop);
    }
    return map;
  }

  private static ProjectQuota createProjectQuota(ProjectQuotaDTO csDTO) {
    ProjectQuota hop = new ProjectQuota(csDTO.getProjectName(), csDTO.
            getRemainingQuota(), csDTO.getTotalUsedQuota());
    return hop;
  }

  @Override
  public void addAll(Collection<ProjectQuota> projectsQuota) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ProjectQuotaDTO> toAdd = new ArrayList<>();
    for (ProjectQuota projectQuota : projectsQuota) {
      toAdd.add(createPersistable(projectQuota, session));
    }
    session.savePersistentAll(toAdd);
    session.release(toAdd);

  }

  private ProjectQuotaDTO createPersistable(ProjectQuota hopPQ,
          HopsSession session) throws StorageException {
    ProjectQuotaDTO pqDTO = session.newInstance(ProjectQuotaDTO.class);
    //Set values to persist new ContainerStatus
    pqDTO.setProjectName(hopPQ.getProjectid());
    pqDTO.setRemainingQuota(hopPQ.getRemainingQuota());
    pqDTO.setTotalUsedQuota(hopPQ.getTotalUsedQuota());

    return pqDTO;

  }
}
