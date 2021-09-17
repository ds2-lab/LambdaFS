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
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.entity.MisReplicatedRange;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.List;

public class MisReplicatedRangeQueueClusterj
    implements TablesDef.MisReplicatedRangeQueueTableDef,
    MisReplicatedRangeQueueDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface MisReplicatedRangeQueueDTO {

    @PrimaryKey
    @Column(name = NNID)
    long getNnId();

    void setNnId(long nnId);
    
    @Column(name = START_INDEX)
    long getStartIndex();

    void setStartIndex(long startIndex);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static String SEPERATOR = "-";

  @Override
  public void insert(MisReplicatedRange range) throws StorageException {
    HopsSession session = connector.obtainSession();
    MisReplicatedRangeQueueDTO dto = null;
    try {
      dto = createPersistable(range, session);
      session.savePersistent(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    } finally {
      session.release(dto);
    }

  }

  @Override
  public void remove(MisReplicatedRange range) throws StorageException {
    HopsSession session = connector.obtainSession();
    MisReplicatedRangeQueueDTO oldR = null;
    try {
      oldR = createPersistable(range, session);
      session.deletePersistent(oldR);
    } catch (Exception e) {
      throw new StorageException(e);
    }finally {
      session.release(oldR);
    }
  }
  
  @Override
  public void remove(List<MisReplicatedRange> ranges) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<MisReplicatedRangeQueueDTO> oldRs = new ArrayList<>(ranges.size());
    try {
      for(MisReplicatedRange range: ranges){
        oldRs.add(createPersistable(range, session));
      }
      session.deletePersistentAll(oldRs);
    } catch (Exception e) {
      throw new StorageException(e);
    }finally {
      session.release(oldRs);
    }
  }

  @Override
  public List<MisReplicatedRange> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<MisReplicatedRangeQueueDTO> dobj = qb.createQueryDefinition(MisReplicatedRangeQueueDTO.class);
    HopsQuery<MisReplicatedRangeQueueDTO> query = session.createQuery(dobj);

    List<MisReplicatedRangeQueueDTO> dtos = query.getResultList();
    List<MisReplicatedRange> directives = convert(dtos);
    session.release(dtos);
    return directives;
  }
  
  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  private MisReplicatedRangeQueueDTO createPersistable(MisReplicatedRange range, HopsSession session) throws
      StorageException {
    MisReplicatedRangeQueueDTO dto = session.newInstance(MisReplicatedRangeQueueDTO.class);
    dto.setNnId(range.getNnId());
    dto.setStartIndex(range.getStartIndex());
    return dto;
  }
  
  private List<MisReplicatedRange> convert(List<MisReplicatedRangeQueueDTO> dtos){
    List<MisReplicatedRange> result = new ArrayList<>(dtos.size());
    for(MisReplicatedRangeQueueDTO dto: dtos){
      result.add(new MisReplicatedRange(dto.getNnId(), dto.getStartIndex()));
    }
    return result;
  }
}
