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
package io.hops.metadata.ndb.dalimpl.configurationstore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.ConfMutationDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import java.util.ArrayList;
import java.util.List;

public class ConfMutationClusterJ
    implements TablesDef.ConfMutationTableDef,
    ConfMutationDataAccess<byte[]> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ConfMutationDTO {

    @PrimaryKey
    @Column(name = INDEX)
    int getIndex();

    void setIndex(int index);

    @Column(name = MUTATION)
    byte[] getMutation();

    void setMutation(byte[] mutation);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  public List<byte[]> get() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ConfMutationDTO> dobj = qb.createQueryDefinition(ConfMutationDTO.class);
    HopsQuery<ConfMutationDTO> query = session.createQuery(dobj);
    List<ConfMutationDTO> queryResults = query.getResultList();
    List<byte[]> result = createList(queryResults);
    session.release(queryResults);
    return result;
  }

  //TODO find a better way to do this
  public void set(List<byte[]> mutations) throws StorageException {
    HopsSession session = connector.obtainSession();
    //empty the table
    session.deletePersistentAll(ConfMutationDTO.class);

    //set the new values
    List<ConfMutationDTO> toAdd = createPersistable(mutations, session);
    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  private List<ConfMutationDTO> createPersistable(List<byte[]> mutations, HopsSession session) throws StorageException {
    List<ConfMutationDTO> dtos = new ArrayList<>();
    int i = 0;
    for (byte[] mutation : mutations) {
      ConfMutationDTO dto = session.newInstance(ConfMutationDTO.class);
      dto.setIndex(i);
      dto.setMutation(mutation);
      i++;
      dtos.add(dto);
    }

    return dtos;
  }

  private List<byte[]> createList(List<ConfMutationDTO> results) {
    List<byte[]> reservationStates = new ArrayList<>(results.size());
    for (ConfMutationDTO persistable : results) {
      reservationStates.add(persistable.getIndex(), persistable.getMutation());
    }
    return reservationStates;
  }

}
