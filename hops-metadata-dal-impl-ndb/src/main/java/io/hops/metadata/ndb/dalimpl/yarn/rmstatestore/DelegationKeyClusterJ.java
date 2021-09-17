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
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationKey;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class DelegationKeyClusterJ
    implements TablesDef.DelegationKeyTableDef, DelegationKeyDataAccess<DelegationKey> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface DelegationKeyDTO {

    @PrimaryKey
    @Column(name = KEY)
    int getkey();

    void setkey(int key);

    @Column(name = DELEGATIONKEY)
    byte[] getdelegationkey();

    void setdelegationkey(byte[] delegationkey);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void remove(DelegationKey removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    DelegationKeyDTO dto = session
        .newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class,
            removed.getKey());
    session.deletePersistent(dto);
    session.release(dto);
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(DelegationKeyDTO.class);
  }
  
  @Override
  public void add(DelegationKey hopDelegationKey)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    DelegationKeyDTO dto = createPersistable(hopDelegationKey, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public List<DelegationKey> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DelegationKeyDTO> dobj = qb.createQueryDefinition(
            DelegationKeyDTO.class);
    HopsQuery<DelegationKeyDTO> query = session.createQuery(dobj);
    List<DelegationKeyDTO> queryResults = query.getResultList();

    List<DelegationKey> result = createHopDelegationKeyList(queryResults);
    session.release(queryResults);
    return result;

  }

  private DelegationKey createHopDelegationKey(
      DelegationKeyDTO delegationKeyDTO) throws StorageException {
    try {
      return new DelegationKey(delegationKeyDTO.getkey(), CompressionUtils.
          decompress(delegationKeyDTO.getdelegationkey()));
    } catch (IOException | DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private List<DelegationKey> createHopDelegationKeyList(
      List<DelegationKeyClusterJ.DelegationKeyDTO> list)
      throws StorageException {
    List<DelegationKey> hopList = new ArrayList<>();
    for (DelegationKeyClusterJ.DelegationKeyDTO dto : list) {
      hopList.add(createHopDelegationKey(dto));
    }
    return hopList;
  }

  private DelegationKeyDTO createPersistable(DelegationKey hop,
      HopsSession session) throws StorageException {
    DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = session.
        newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class);
    delegationKeyDTO.setkey(hop.getKey());
    try {
      delegationKeyDTO.setdelegationkey(CompressionUtils.compress(hop.
          getDelegationkey()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return delegationKeyDTO;
  }
}
