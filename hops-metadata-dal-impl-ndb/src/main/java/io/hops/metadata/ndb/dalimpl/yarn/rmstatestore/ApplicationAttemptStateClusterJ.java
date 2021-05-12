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
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

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
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

public class ApplicationAttemptStateClusterJ
    implements TablesDef.ApplicationAttemptStateTableDef,
    ApplicationAttemptStateDataAccess<ApplicationAttemptState> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ApplicationAttemptStateDTO {

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @Column(name = APPLICATIONATTEMPTSTATE)
    byte[] getapplicationattemptstate();

    void setapplicationattemptstate(byte[] applicationattemptstate);

    @Column(name = TRAKINGURL)
    String gettrakingurl();

    void settrakingurl(String trakingurl);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, List<ApplicationAttemptState>> getAll()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ApplicationAttemptStateDTO> dobj = qb.
        createQueryDefinition(ApplicationAttemptStateDTO.class);
    HopsQuery<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
    List<ApplicationAttemptStateDTO> queryResults = query.getResultList();

    Map<String, List<ApplicationAttemptState>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public List<ApplicationAttemptState> getByAppId(String appId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ApplicationAttemptStateDTO> dobj = qb.
        createQueryDefinition(ApplicationAttemptStateDTO.class);
    HopsPredicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
    HopsQuery<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
    query.setParameter("applicationid", appId);
    List<ApplicationAttemptStateDTO> queryResults = query.getResultList();

    List<ApplicationAttemptState> result = createList(queryResults);
    session.release(queryResults);
    return result;
  }
  
  @Override
  public ApplicationAttemptState get(String appId, String appAttemptId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    String[] key = new String[2];
    key[0] = appId;
    key[1] = appAttemptId;
    ApplicationAttemptStateDTO appStateDTO = session.find(ApplicationAttemptStateDTO.class, key);

    ApplicationAttemptState result =  createHopApplicationAttemptState(appStateDTO);
    session.release(appStateDTO);
    return result;
  }
  
  @Override
  public void add(ApplicationAttemptState entry)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
  }

  @Override
  public void removeAll(Collection<ApplicationAttemptState> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ApplicationAttemptStateDTO> toRemove =
        new ArrayList<>();
    for (ApplicationAttemptState hop : removed) {
      Object[] objarr = new Object[2];
      objarr[0] = hop.getApplicationId();
      objarr[1] = hop.getApplicationattemptid();
      toRemove.add(session.newInstance(
          ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class,
          objarr));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }
  
    @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(ApplicationAttemptStateDTO.class);
  }
  
  private ApplicationAttemptState createHopApplicationAttemptState(
      ApplicationAttemptStateDTO entry) throws StorageException {
    try {
      return new ApplicationAttemptState(entry.getapplicationid(),
          entry.getapplicationattemptid(),
          CompressionUtils.decompress(entry.getapplicationattemptstate()),
          entry.gettrakingurl());
    } catch (IOException | DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private ApplicationAttemptStateDTO createPersistable(
      ApplicationAttemptState hop, HopsSession session)
      throws StorageException {
    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO
        applicationAttemptStateDTO = session.newInstance(
        ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class);

    applicationAttemptStateDTO.setapplicationid(hop.getApplicationId());
    applicationAttemptStateDTO.setapplicationattemptid(hop.
        getApplicationattemptid());
    applicationAttemptStateDTO.settrakingurl(hop.getTrakingURL());
    try {
      applicationAttemptStateDTO.setapplicationattemptstate(CompressionUtils.
          compress(hop.
              getApplicationattemptstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return applicationAttemptStateDTO;
  }

  private Map<String, List<ApplicationAttemptState>> createMap(
      List<ApplicationAttemptStateDTO> results) throws StorageException {
    Map<String, List<ApplicationAttemptState>> map =
        new HashMap<>();
    for (ApplicationAttemptStateDTO persistable : results) {
      ApplicationAttemptState hop =
          createHopApplicationAttemptState(persistable);
      if (map.get(hop.getApplicationId()) == null) {
        map.put(hop.getApplicationId(),
            new ArrayList<ApplicationAttemptState>());
      }
      map.get(hop.getApplicationId()).add(hop);
    }
    return map;
  }
  
  private List<ApplicationAttemptState> createList(List<ApplicationAttemptStateDTO> results) throws StorageException {
    List<ApplicationAttemptState> list = new ArrayList<>();
    for (ApplicationAttemptStateDTO persistable : results) {
      ApplicationAttemptState hop = createHopApplicationAttemptState(persistable);
      list.add(hop);
    }
    return list;
  }
}
