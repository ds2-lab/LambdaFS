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
import io.hops.metadata.yarn.dal.ReservationStateDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.ReservationState;
import java.util.ArrayList;
import java.util.List;

public class ReservationStateClusterJ implements TablesDef.ReservationStateTableDef,
    ReservationStateDataAccess<ReservationState> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ReservationStateDTO {

    @PrimaryKey
    @Column(name = PLANNAME)
    String getPlanName();

    void setPlanName(String planName);

    @PrimaryKey
    @Column(name = RESERVATIONIDNAME)
    String getReservationIdName();

    void setReservationIdName(String reservationIdName);

    @PrimaryKey
    @Column(name = RESERVATIONSTATE)
    byte[] getReservationState();

    void setReservationState(byte[] reservationState);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<ReservationState> getAll() throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReservationStateDTO> dobj = qb.createQueryDefinition(ReservationStateDTO.class);
    HopsQuery<ReservationStateDTO> query = session.createQuery(dobj);
    List<ReservationStateDTO> queryResults = query.getResultList();
    List<ReservationState> result = createList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void add(ReservationState state) throws StorageException {
    HopsSession session = connector.obtainSession();
    ReservationStateDTO toModify = createPersistable(state, session);
    session.savePersistent(toModify);
    session.release(toModify);
  }

  @Override
  public void remove(ReservationState state) throws StorageException {
    HopsSession session = connector.obtainSession();
    ReservationStateDTO toRemove = createPersistable(state, session);
    session.deletePersistent(toRemove);
    session.release(toRemove);
  }

  private ReservationStateDTO createPersistable(ReservationState hop, HopsSession session) throws StorageException {
    ReservationStateDTO dto = session.newInstance(ReservationStateDTO.class);
    //Set values to persist new ContainerStatus
    dto.setPlanName(hop.getPlanName());
    dto.setReservationIdName(hop.getReservationIdName());
    dto.setReservationState(hop.getState());

    return dto;
  }

  private List<ReservationState> createList(List<ReservationStateDTO> results) {
    List<ReservationState> reservationStates = new ArrayList<>(results.size());
    for (ReservationStateDTO persistable : results) {
      reservationStates.add(createReservationState(persistable));
    }
    return reservationStates;
  }

  private ReservationState createReservationState(ReservationStateDTO dto) {
    return new ReservationState(dto.getReservationState(), dto.getPlanName(), dto.getReservationIdName());
  }
}
