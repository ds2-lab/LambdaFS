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
import io.hops.metadata.hdfs.dal.ConfDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import java.util.ArrayList;
import java.util.List;

public class ConfClusterJ implements TablesDef.ConfTableDef, ConfDataAccess<byte[]> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ConfDTO {

    @PrimaryKey
    @Column(name = INDEX)
    int getIndex();

    void setIndex(int index);

    @Column(name = CONF)
    byte[] getConf();

    void setConf(byte[] conf);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  public byte[] get() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ConfDTO> dobj = qb.createQueryDefinition(ConfDTO.class);
    HopsQuery<ConfDTO> query = session.createQuery(dobj);
    List<ConfDTO> queryResults = query.getResultList();
    byte[] result = create(queryResults);
    session.release(queryResults);
    return result;
  }

  //TODO find a better way to do this
  public void set(byte[] conf) throws StorageException {
    HopsSession session = connector.obtainSession();
    //empty the table
    session.deletePersistentAll(ConfDTO.class);

    //set the new values
    ConfDTO toAdd = createPersistable(conf, session);
    session.savePersistent(toAdd);
    session.release(toAdd);
  }

  private ConfDTO createPersistable(byte[] conf, HopsSession session) throws StorageException {
    ConfDTO dto = session.newInstance(ConfDTO.class);
    dto.setIndex(0);
    dto.setConf(conf);
    return dto;
  }

  private byte[] create(List<ConfDTO> results) {
    List<byte[]> reservationStates = new ArrayList<>(results.size());
    for (ConfDTO persistable : results) {
      if(persistable==null){
        continue;
      }
      reservationStates.add(persistable.getIndex(), persistable.getConf());
    }
    if(reservationStates.isEmpty()){
      return null;
    }
    return reservationStates.get(0);
  }

}
