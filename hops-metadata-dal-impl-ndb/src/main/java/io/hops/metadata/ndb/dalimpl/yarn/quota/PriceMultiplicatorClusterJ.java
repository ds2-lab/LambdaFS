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
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.yarn.TablesDef;


public class PriceMultiplicatorClusterJ implements
        TablesDef.PriceMultiplicatorTableDef,
        PriceMultiplicatorDataAccess<PriceMultiplicator> {

  private static final Log LOG = LogFactory.getLog(
          PriceMultiplicatorClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface PriceMultiplicatorDTO {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = MULTIPLICATOR)
    float getMultiplicator();

    void setMultiplicator(float multiplicator);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> getAll() throws
          StorageException {
    LOG.debug("HOP :: ClusterJ PriceMultiplicator.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PriceMultiplicatorClusterJ.PriceMultiplicatorDTO> dobj
            = qb.createQueryDefinition(
                    PriceMultiplicatorClusterJ.PriceMultiplicatorDTO.class);
    HopsQuery<PriceMultiplicatorClusterJ.PriceMultiplicatorDTO> query = session.
            createQuery(dobj);

    List<PriceMultiplicatorClusterJ.PriceMultiplicatorDTO> queryResults = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ PriceMultiplicator.getAll - STOP");
    Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> result = createMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  public static Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> createMap(
          List<PriceMultiplicatorClusterJ.PriceMultiplicatorDTO> results) {
    Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> map
            = new HashMap<>();
    for (PriceMultiplicatorClusterJ.PriceMultiplicatorDTO persistable : results) {
      PriceMultiplicator hop = createHopPriceMultiplicator(persistable);
      map.put(hop.getId(), hop);
    }
    return map;
  }

  private static PriceMultiplicator createHopPriceMultiplicator(
          PriceMultiplicatorClusterJ.PriceMultiplicatorDTO csDTO) {
    PriceMultiplicator hop = new PriceMultiplicator(PriceMultiplicator.MultiplicatorType.
            valueOf(csDTO.getId()), csDTO.getMultiplicator());
    return hop;
  }

  @Override
  public void add(PriceMultiplicator yarnRunningPrice) throws StorageException {
    HopsSession session = connector.obtainSession();
    PriceMultiplicatorClusterJ.PriceMultiplicatorDTO toAdd = createPersistable(
            yarnRunningPrice, session);
    session.savePersistent(toAdd);
    session.release(toAdd);
  }

  private PriceMultiplicatorClusterJ.PriceMultiplicatorDTO createPersistable(
          PriceMultiplicator hopPQ,
          HopsSession session) throws StorageException {
    PriceMultiplicatorClusterJ.PriceMultiplicatorDTO pqDTO = session.newInstance(
            PriceMultiplicatorClusterJ.PriceMultiplicatorDTO.class);
    //Set values to persist new PriceMultiplicatorDTO
    pqDTO.setId(hopPQ.getId().name());
    pqDTO.setMultiplicator(hopPQ.getValue());

    return pqDTO;

  }

}
