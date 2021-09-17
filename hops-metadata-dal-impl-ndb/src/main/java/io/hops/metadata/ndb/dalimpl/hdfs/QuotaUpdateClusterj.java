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
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QuotaUpdateClusterj
    implements TablesDef.QuotaUpdateTableDef, QuotaUpdateDataAccess<QuotaUpdate> {

  static final Log LOG = LogFactory.getLog(QuotaUpdateClusterj.class);
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface QuotaUpdateDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long id);

    @Column(name = NAMESPACE_DELTA)
    long getNamespaceDelta();

    void setNamespaceDelta(long namespaceDelta);

    @Column(name = STORAGE_SPACE_DELTA)
    long getStorageSpaceDelta();

    void setStorageSpaceDelta(long StorageSpaceDelta);
    
    @Column(name = TYPESPACE_DELTA_DISK)
    long getTypeSpaceDeltaDisk();
    
    void setTypeSpaceDeltaDisk(long delta);
    
    @Column(name = TYPESPACE_DELTA_SSD)
    long getTypeSpaceDeltaSSD();
    
    void setTypeSpaceDeltaSSD(long delta);
    
    @Column(name = TYPESPACE_DELTA_RAID5)
    long getTypeSpaceDeltaRaid5();
    
    void setTypeSpaceDeltaRaid5(long delta);
    
    @Column(name = TYPESPACE_DELTA_ARCHIVE)
    long getTypeSpaceDeltaArchive();
    
    void setTypeSpaceDeltaArchive(long delta);

    @Column(name = TYPESPACE_DELTA_DB)
    long getTypeSpaceDeltaDb();

    void setTypeSpaceDeltaDb(long delta);
    
    @Column(name = TYPESPACE_DELTA_PROVIDED)
    long getTypeSpaceDeltaProvided();

    void setTypeSpaceDeltaProvided(long delta);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();

  @Override 
  public QuotaUpdate findByKey(int id, long inodeId) throws StorageException{
    HopsSession dbSession = connector.obtainSession();
    Object[] keys = new Object[]{id, inodeId};
    QuotaUpdateDTO dto = (QuotaUpdateDTO) dbSession.find(QuotaUpdateDTO.class, keys);
    if (dto != null) {
      return convertAndRelease(dbSession, dto);
    }
    return null;
  }
  
  @Override
  public void prepare(Collection<QuotaUpdate> added,
      Collection<QuotaUpdate> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<QuotaUpdateDTO> changes = new ArrayList<>();
    List<QuotaUpdateDTO> deletions = new ArrayList<>();
    try {
      if (removed != null) {
        for (QuotaUpdate update : removed) {
          QuotaUpdateDTO persistable = createPersistable(update, session);
          deletions.add(persistable);
        }
      }
      if (added != null) {
        for (QuotaUpdate update : added) {
          QuotaUpdateDTO persistable = createPersistable(update, session);
          changes.add(persistable);
        }
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private static final String FIND_QUERY =
      "SELECT * FROM " + TABLE_NAME + " ORDER BY " + ID + " LIMIT ?";

  @Override
  public List<QuotaUpdate> findLimited(int limit) throws StorageException {
    ArrayList<QuotaUpdate> resultList;
    PreparedStatement s = null;
    ResultSet result = null;
    try {
      Connection conn = mysqlConnector.obtainSession();
      s = conn.prepareStatement(FIND_QUERY);
      s.setInt(1, limit);
      result = s.executeQuery();
      resultList = new ArrayList<>();

      while (result.next()) {
        int id = result.getInt(ID);
        int inodeId = result.getInt(INODE_ID);
        int namespaceDelta = result.getInt(NAMESPACE_DELTA);
        long diskspaceDelta = result.getLong(STORAGE_SPACE_DELTA);
        Map<QuotaUpdate.StorageType, Long> typeSpaceDelta = new HashMap<>();
        typeSpaceDelta.put(QuotaUpdate.StorageType.DISK, result.getLong(TYPESPACE_DELTA_DISK));
        typeSpaceDelta.put(QuotaUpdate.StorageType.SSD, result.getLong(TYPESPACE_DELTA_SSD));
        typeSpaceDelta.put(QuotaUpdate.StorageType.RAID5, result.getLong(TYPESPACE_DELTA_RAID5));
        typeSpaceDelta.put(QuotaUpdate.StorageType.ARCHIVE, result.getLong(TYPESPACE_DELTA_ARCHIVE));
        typeSpaceDelta.put(QuotaUpdate.StorageType.DB, result.getLong(TYPESPACE_DELTA_DB));
        typeSpaceDelta.put(QuotaUpdate.StorageType.PROVIDED, result.getLong(TYPESPACE_DELTA_PROVIDED));
        resultList
            .add(new QuotaUpdate(id, inodeId, namespaceDelta, diskspaceDelta, typeSpaceDelta));
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException ex) {
          LOG.warn("Exception when closing the PrepareStatement", ex);
        }
      }
      if (result != null) {
        try {
          result.close();
        } catch (SQLException ex) {
          LOG.warn("Exception when closing the REsultSet", ex);
        }
      }
      mysqlConnector.closeSession();
    }
    return resultList;
  }

  private QuotaUpdateDTO createPersistable(QuotaUpdate update,
      HopsSession session) throws StorageException {
    QuotaUpdateDTO dto = session.newInstance(QuotaUpdateDTO.class);
    dto.setId(update.getId());
    dto.setInodeId(update.getInodeId());
    dto.setNamespaceDelta(update.getNamespaceDelta());
    dto.setStorageSpaceDelta(update.getStorageSpaceDelta());
    dto.setTypeSpaceDeltaDisk(update.getTypeSpaces().get(QuotaUpdate.StorageType.DISK));
    dto.setTypeSpaceDeltaSSD(update.getTypeSpaces().get(QuotaUpdate.StorageType.SSD));
    dto.setTypeSpaceDeltaRaid5(update.getTypeSpaces().get(QuotaUpdate.StorageType.RAID5));
    dto.setTypeSpaceDeltaArchive(update.getTypeSpaces().get(QuotaUpdate.StorageType.ARCHIVE));
    dto.setTypeSpaceDeltaDb(update.getTypeSpaces().get(QuotaUpdate.StorageType.DB));
    dto.setTypeSpaceDeltaProvided(update.getTypeSpaces().get(QuotaUpdate.StorageType.PROVIDED));
    return dto;
  }

  private List<QuotaUpdate> convertAndRelease(HopsSession session,
      List<QuotaUpdateDTO> list) throws StorageException {
    List<QuotaUpdate> result = new ArrayList<>();
    for (QuotaUpdateDTO dto : list) {
      result.add(convertAndRelease(session, dto));
    }
    return result;
  }

  private QuotaUpdate convertAndRelease(HopsSession session, QuotaUpdateDTO dto) throws StorageException{
    Map<QuotaUpdate.StorageType, Long> typeSpaceDelta = new HashMap<>();
      typeSpaceDelta.put(QuotaUpdate.StorageType.DISK, dto.getTypeSpaceDeltaDisk());
      typeSpaceDelta.put(QuotaUpdate.StorageType.SSD, dto.getTypeSpaceDeltaSSD());
      typeSpaceDelta.put(QuotaUpdate.StorageType.RAID5, dto.getTypeSpaceDeltaRaid5());
      typeSpaceDelta.put(QuotaUpdate.StorageType.ARCHIVE, dto.getTypeSpaceDeltaArchive());
      typeSpaceDelta.put(QuotaUpdate.StorageType.DB, dto.getTypeSpaceDeltaDb());
      typeSpaceDelta.put(QuotaUpdate.StorageType.PROVIDED, dto.getTypeSpaceDeltaProvided());
      QuotaUpdate result = new QuotaUpdate(dto.getId(), dto.getInodeId(),
          dto.getNamespaceDelta(), dto.getStorageSpaceDelta(), typeSpaceDelta);
      session.release(dto);
      return result;
  }
  
  private static final String INODE_ID_PARAM = "inodeId";

  @Override
  public List<QuotaUpdate> findByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<QuotaUpdateDTO> dobj =
        qb.createQueryDefinition(QuotaUpdateDTO.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param(INODE_ID_PARAM));
    dobj.where(pred1);
    HopsQuery<QuotaUpdateDTO> query = session.createQuery(dobj);
    query.setParameter(INODE_ID_PARAM, inodeId);

    List<QuotaUpdateDTO> results = query.getResultList();
    return convertAndRelease(session, results);
  }

  @Override
  public int getCount() throws StorageException {
    int count = MySQLQueryHelper.countAll(TablesDef.QuotaUpdateTableDef.TABLE_NAME);
    return count;
  }
}
