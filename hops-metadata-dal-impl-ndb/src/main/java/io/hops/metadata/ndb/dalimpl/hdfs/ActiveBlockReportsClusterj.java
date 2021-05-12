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
import static io.hops.metadata.hdfs.TablesDef.SafeBlocksTableDef.TABLE_NAME;
import io.hops.metadata.hdfs.dal.ActiveBlockReportsDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.entity.ActiveBlockReport;
import io.hops.metadata.hdfs.entity.User;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ActiveBlockReportsClusterj implements TablesDef.ActiveBlockReports,
        ActiveBlockReportsDataAccess<ActiveBlockReport> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ActiveBlockReportDTO {

    @PrimaryKey
    @Column(name = DN_ADDRESS)
    String getDnAddress();
    void setDnAddress(String dnAddress);

    @Column(name = NN_ID)
    long getNnId();
    void setNnId(long nnId);

    @Column(name = NN_ADDRESS)
    String getNnAddress();
    void setNnAddress(String nnAddress);

    @Column(name = START_TIME)
    long getStartTime();
    void setStartTime(long startTime);

    @Column(name = NUM_BLOCKS)
    long getNumBlocks();
    void setNumBlocks(long numBlocks);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countActiveRports() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public void addActiveReport(ActiveBlockReport abr) throws StorageException {
    HopsSession session = connector.obtainSession();
    ActiveBlockReportDTO dto = convertToDto(session, abr);
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public void removeActiveReport(ActiveBlockReport abr) throws StorageException {
    HopsSession session = connector.obtainSession();
    ActiveBlockReportDTO dto = convertToDto(session, abr);
    session.deletePersistent(dto);
    session.release(dto);
  }

  private ActiveBlockReportDTO convertToDto(HopsSession session,
                                           ActiveBlockReport abr) throws StorageException {
    ActiveBlockReportDTO dto = session.newInstance(ActiveBlockReportDTO.class);
    dto.setDnAddress(abr.getDnAddress());
    dto.setNnId(abr.getNnId());
    dto.setNnAddress(abr.getNnAddress());
    dto.setStartTime(abr.getStartTime());
    dto.setNumBlocks(abr.getNumBlocks());
    return dto;
  }

  @Override
  public List<ActiveBlockReport> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<ActiveBlockReportDTO> query =
            session.createQuery(qb.createQueryDefinition(ActiveBlockReportDTO.class));
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public ActiveBlockReport getActiveBlockReport(ActiveBlockReport abr) throws StorageException{
    HopsSession session = connector.obtainSession();
    ActiveBlockReportDTO dto = session.find(ActiveBlockReportDTO.class, abr.getDnAddress());
    ActiveBlockReport result = null;
    if(dto!=null){
      result = convertAndRelease(session, dto);
    }
    return result;
  }
  
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }
  
  @Override
  public void removeAll() throws StorageException {
    try {
      while (countAll() != 0) {
        MysqlServerConnector.truncateTable(TABLE_NAME, 10000);
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    }
  }

  private List<ActiveBlockReport> convertAndRelease(HopsSession session,
                                                    Collection<ActiveBlockReportDTO> dtos)
          throws StorageException {
    List<ActiveBlockReport> list = new ArrayList<>();
    for (ActiveBlockReportDTO dto : dtos) {
      list.add(convertAndRelease(session, dto));
    }
    return list;
  }

  private ActiveBlockReport convertAndRelease(HopsSession session, ActiveBlockReportDTO abrDto)
          throws StorageException {
    ActiveBlockReport abr = new ActiveBlockReport(abrDto.getDnAddress(), abrDto.getNnId(),
            abrDto.getNnAddress(), abrDto.getStartTime(), abrDto.getNumBlocks() );
    session.release(abrDto);
    return abr;
  }
}
