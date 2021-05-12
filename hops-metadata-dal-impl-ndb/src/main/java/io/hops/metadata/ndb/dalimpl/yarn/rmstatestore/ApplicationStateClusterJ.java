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
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class ApplicationStateClusterJ implements
    TablesDef.ApplicationStateTableDef,
    ApplicationStateDataAccess<ApplicationState> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ApplicationStateDTO {

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @Column(name = APPSTATE)
    byte[] getappstate();

    void setappstate(byte[] appstate);

    @Column(name = USER)
    String getappuser();

    void setappuser(String user);

    @Column(name = NAME)
    String getappname();

    void setappname(String name);

    @Column(name = SMSTATE)
    String getappsmstate();

    void setappsmstate(String appstate);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public ApplicationState findByApplicationId(String id)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    ApplicationStateDTO appStateDTO = session.find(ApplicationStateDTO.class, id);

    ApplicationState result =  createHopApplicationState(appStateDTO);
    session.release(appStateDTO);
    return result;
  }
  
  @Override
  public List<ApplicationState> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ApplicationStateDTO> dobj = qb.createQueryDefinition(
            ApplicationStateDTO.class);
      //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
    //dobj.where(pred1);
    HopsQuery<ApplicationStateDTO> query = session.createQuery(dobj);
    //query.setParameter("applicationid", applicationid);
    List<ApplicationStateDTO> queryResults = query.getResultList();
    List<ApplicationState> result = createHopApplicationStateList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void add(ApplicationState toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    ApplicationStateDTO dto = createPersistable(toAdd, session);
    session.savePersistent(dto);
    session.flush();
    session.release(dto);
  }

  @Override
  public void remove(ApplicationState toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    ApplicationStateDTO dto = session.newInstance(ApplicationStateDTO.class, toRemove.
                getApplicationId());
    session.deletePersistent(dto);
    session.release(dto);
  }
  
  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(ApplicationStateDTO.class);
  }
  
  private ApplicationState createHopApplicationState(
      ApplicationStateDTO appStateDTO) throws StorageException {
    if (appStateDTO != null) {
      ApplicationState state = null;
      try {
        state = new ApplicationState(appStateDTO.getapplicationid(),
            CompressionUtils.decompress(appStateDTO.getappstate()), appStateDTO.
            getappuser(), appStateDTO.getappname(),
            appStateDTO.getappsmstate());
      } catch (IOException | DataFormatException e) {
        throw new StorageException(e);
      }
      return state;
    } else {
      return null;
    }
  }

  private List<ApplicationState> createHopApplicationStateList(
      List<ApplicationStateDTO> list) throws StorageException {
    List<ApplicationState> hopList = new ArrayList<>();
    for (ApplicationStateDTO dto : list) {
      hopList.add(createHopApplicationState(dto));
    }
    return hopList;

  }

  private ApplicationStateDTO createPersistable(ApplicationState hop,
      HopsSession session) throws StorageException {
    ApplicationStateDTO appStateDTO =
        session.newInstance(ApplicationStateClusterJ.ApplicationStateDTO.class);
    appStateDTO.setapplicationid(hop.getApplicationid());
    try {
      appStateDTO.setappstate(CompressionUtils.compress(hop.getAppstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    appStateDTO.setappuser(hop.getUser());
    String name = hop.getName();
    if(name !=null && name.length()>200){
      name = name.substring(0, 200);
    }
    appStateDTO.setappname(name);
    appStateDTO.setappsmstate(hop.getState());

    return appStateDTO;
  }
}
