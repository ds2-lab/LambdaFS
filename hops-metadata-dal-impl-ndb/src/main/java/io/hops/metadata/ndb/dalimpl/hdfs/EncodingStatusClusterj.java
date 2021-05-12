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
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.mysqlserver.CountHelper;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class EncodingStatusClusterj implements TablesDef.EncodingStatusTableDef,
    EncodingStatusDataAccess<EncodingStatus> {

  static final Log LOG = LogFactory.getLog(EncodingStatusClusterj.class);

  private ClusterjConnector clusterjConnector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();
   private final static int NOT_FOUND = -1000;

  @PersistenceCapable(table = TABLE_NAME)
  public interface EncodingStatusDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = STATUS)
    Integer getStatus();

    void setStatus(Integer status);

    @Column(name = CODEC)
    String getCodec();

    void setCodec(String codec);

    @Column(name = TARGET_REPLICATION)
    Short getTargetReplication();

    void setTargetReplication(Short targetReplication);

    @Column(name = STATUS_MODIFICATION_TIME)
    Long getStatusModificationTime();

    void setStatusModificationTime(Long modificationTime);

    @Index
    @Column(name = PARITY_INODE_ID)
    long getParityInodeId();

    // Long type not possible because of index
    void setParityInodeId(long inodeId);

    @Column(name = PARITY_STATUS)
    Integer getParityStatus();

    void setParityStatus(Integer status);

    @Column(name = PARITY_STATUS_MODIFICATION_TIME)
    Long getParityStatusModificationTime();

    void setParityStatusModificationTime(Long modificationTime);

    @Column(name = PARITY_FILE_NAME)
    String getParityFileName();

    void setParityFileName(String parityFileName);

    @Column(name = LOST_BLOCKS)
    int getLostBlockCount();

    void setLostBlockCount(int n);

    @Column(name = LOST_PARITY_BLOCKS)
    int getLostParityBlockCount();

    void setLostParityBlockCount(int n);

    @Column(name = REVOKED)
    byte getRevoked();

    void setRevoked(byte revoked);
  }

  @Override
  public void add(EncodingStatus status) throws StorageException {
    LOG.info("ADD " + status.toString());
    EncodingStatusDto dto = null;
    HopsSession session = clusterjConnector.obtainSession();
    try {
      dto = session.newInstance(EncodingStatusDto.class);
      copyState(status, dto);
      session.savePersistent(dto);
    } finally {
      session.release(dto);
    }
  }

  @Override
  public void update(EncodingStatus status) throws StorageException {
    LOG.info("UPDATE " + status.toString());
    HopsSession session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = null;
    try {
      dto = session.newInstance(EncodingStatusDto.class);
      copyState(status, dto);
      session.savePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  @Override
  public void delete(EncodingStatus status) throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = null;
    try {
      dto = session.newInstance(EncodingStatusDto.class);
      copyState(status, dto);
      LOG.info("Delete " + status);
      session.deletePersistent(dto);
    } finally {
      session.release(dto);
    }
  }

  private void copyState(EncodingStatus status, EncodingStatusDto dto) {
    Long inodeId = status.getInodeId();
    if (inodeId != null) {
      dto.setInodeId(inodeId);
    }
    EncodingStatus.Status sourceStatus = status.getStatus();
    if (sourceStatus != null) {
      dto.setStatus(sourceStatus.ordinal());
    }
    String codec = status.getEncodingPolicy().getCodec();
    if (codec != null) {
      dto.setCodec(codec);
    }
    Short targetReplication = status.getEncodingPolicy().getTargetReplication();
    if (targetReplication != null) {
      dto.setTargetReplication(targetReplication);
    }
    Long statusModificationTime = status.getStatusModificationTime();
    if (statusModificationTime != null) {
      dto.setStatusModificationTime(statusModificationTime);
    }
    Long parityInodeId = status.getParityInodeId();
    if (parityInodeId != null) {
      dto.setParityInodeId(parityInodeId);
    }
    EncodingStatus.ParityStatus parityStatus = status.getParityStatus();
    if (parityStatus != null) {
      dto.setParityStatus(parityStatus.ordinal());
    }
    Long parityStatusModificationTime =
        status.getParityStatusModificationTime();
    if (parityStatusModificationTime != null) {
      dto.setParityStatusModificationTime(parityStatusModificationTime);
    }
    String parityFileName = status.getParityFileName();
    if (parityFileName != null) {
      dto.setParityFileName(parityFileName);
    }
    Integer lostBlocks = status.getLostBlocks();
    if (lostBlocks != null) {
      dto.setLostBlockCount(lostBlocks);
    }
    Integer lostParityBlocks = status.getLostParityBlocks();
    if (lostParityBlocks != null) {
      dto.setLostParityBlockCount(lostParityBlocks);
    }
    Boolean revoked = status.getRevoked();
    if (revoked != null) {
      dto.setRevoked(NdbBoolean.convert(revoked));
    }
  }

  @Override
  public EncodingStatus findByInodeId(long inodeId) throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = session.find(EncodingStatusDto.class, inodeId);
    if (dto == null) {
      return null;
    }
    EncodingStatus es = createHopEncoding(dto);
    session.release(dto);
    return es;
  }
  
  @Override
  public Collection<EncodingStatus> findByInodeIds(Collection<Long> inodeIds)
          throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    List<EncodingStatusDto> dtos = new ArrayList<>();
    try {
      for (long inodeId: inodeIds) {
        EncodingStatusDto dto = session
                .newInstance(EncodingStatusDto.class, inodeId);
        dto.setStatus(NOT_FOUND);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      List<EncodingStatus> statusList = createHopEncodingsIfFound(dtos);
      return statusList;
    } finally {
      session.release(dtos);
    }
  }

  @Override
  public EncodingStatus findByParityInodeId(long inodeId)
      throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    HopsQueryBuilder builder = session.getQueryBuilder();
    HopsQueryDomainType<EncodingStatusDto> domain =
        builder.createQueryDefinition(EncodingStatusDto.class);
    domain.where(
        domain.get("parityInodeId").equal(domain.param(PARITY_INODE_ID)));
    HopsQuery<EncodingStatusDto> query = session.createQuery(domain);
    query.setParameter(PARITY_INODE_ID, inodeId);

    List<EncodingStatusDto> results = query.getResultList();
    assert results.size() <= 1;

    if (results.size() == 0) {
      return null;
    }

    EncodingStatus es = createHopEncoding(results.get(0));
    session.release(results);
    return es;
  }
  
  @Override
  public Collection<EncodingStatus> findByParityInodeIds(List<Long> inodeIds) throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    HopsQueryBuilder builder = session.getQueryBuilder();
    HopsQueryDomainType<EncodingStatusDto> domain = builder.createQueryDefinition(EncodingStatusDto.class);
    domain.where(domain.get("parityInodeId").in(domain.param(PARITY_INODE_ID)));
    HopsQuery<EncodingStatusDto> query = session.createQuery(domain);
    query.setParameter(PARITY_INODE_ID, inodeIds);
    
    List<EncodingStatusDto> results = null;
    try {
      results = query.getResultList();
      
      if (results.size() == 0) {
        return null;
      }
      
      List<EncodingStatus> es = createHopEncodings(results);
      session.release(results);
      return es;
    } finally {
      session.release(results);
    }
  }

  @Override
  public Collection<EncodingStatus> findRequestedEncodings(int limit)
      throws StorageException {
    Collection<EncodingStatus> normalEncodings = findWithStatus(
        EncodingStatus.Status.ENCODING_REQUESTED.ordinal(), limit);
    Collection<EncodingStatus> copyEncodings = findWithStatus(
        EncodingStatus.Status.COPY_ENCODING_REQUESTED.ordinal(), limit);
    ArrayList<EncodingStatus> requests = new ArrayList<>(limit);
    requests.addAll(normalEncodings);
    requests.addAll(copyEncodings);
    Collections.sort(requests, new Comparator<EncodingStatus>() {
      @Override
      public int compare(EncodingStatus o1, EncodingStatus o2) {
        return o1.getStatusModificationTime()
            .compareTo(o2.getStatusModificationTime());
      }
    });
    return requests.subList(0, requests.size() < limit ?
        requests.size() : limit);
  }

  @Override
  public int countRequestedEncodings() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME,
        STATUS + "=" + EncodingStatus.Status.ENCODING_REQUESTED.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findRequestedRepairs(int limit)
      throws StorageException {
    /*
     * Prioritize files with more missing blocks and also prioritize source files over parity files.
     * Finally, prioritize earlier detected failures to prevent starvation.
     */
    final String query =
        "SELECT " + INODE_ID + ", " + STATUS + ", " + CODEC + ", " +
            TARGET_REPLICATION + ", " + PARITY_STATUS + ", " +
            STATUS_MODIFICATION_TIME + ", " + PARITY_STATUS_MODIFICATION_TIME +
            ", " + PARITY_INODE_ID + ", " + PARITY_FILE_NAME + ", " +
            LOST_BLOCKS + ", " + LOST_PARITY_BLOCKS + ", " + LOST_BLOCKS + "+" +
            LOST_PARITY_BLOCKS + " AS " + LOST_BLOCK_SUM + ", " + REVOKED +
            " FROM " + TABLE_NAME + " WHERE " + STATUS + "=" +
            EncodingStatus.Status.REPAIR_REQUESTED.ordinal() + " ORDER BY " +
            LOST_BLOCK_SUM + " DESC, " + LOST_BLOCKS + " DESC, " +
            STATUS_MODIFICATION_TIME + " ASC LIMIT " + limit;
    return find(query);
  }

  @Override
  public int countRequestedRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME,
        STATUS + "=" + EncodingStatus.Status.REPAIR_REQUESTED.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findActiveEncodings()
      throws StorageException {
    return findAllWithStatus(EncodingStatus.Status.ENCODING_ACTIVE.ordinal());
  }

  @Override
  public int countActiveEncodings() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME,
        STATUS + "=" + EncodingStatus.Status.ENCODING_ACTIVE.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findEncoded(int limit)
      throws StorageException {
    return findWithStatus(EncodingStatus.Status.ENCODED.ordinal(), limit);
  }

  @Override
  public int countEncoded() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME,
        STATUS + "=" + EncodingStatus.Status.ENCODED.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findActiveRepairs()
      throws StorageException {
    return findAllWithStatus(EncodingStatus.Status.REPAIR_ACTIVE.ordinal());
  }

  @Override
  public int countActiveRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME,
        STATUS + "=" + EncodingStatus.Status.REPAIR_ACTIVE.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findRequestedParityRepairs(int limit)
      throws StorageException {
    final String queryString =
        "SELECT * FROM %s WHERE %s=%s AND %s!=%s AND %s!=%s ORDER BY %s ASC LIMIT %s";
    String query = String.format(STATUS_QUERY, TABLE_NAME, PARITY_STATUS,
        EncodingStatus.ParityStatus.REPAIR_REQUESTED.ordinal(), STATUS,
        EncodingStatus.Status.REPAIR_ACTIVE.ordinal(), STATUS,
        EncodingStatus.Status.REPAIR_FAILED.ordinal(),
        PARITY_STATUS_MODIFICATION_TIME, limit);
    return find(query);
  }

  @Override
  public int countRequestedParityRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, PARITY_STATUS + "=" +
        EncodingStatus.ParityStatus.REPAIR_REQUESTED.ordinal());
  }

  @Override
  public Collection<EncodingStatus> findActiveParityRepairs()
      throws StorageException {
    return findAllWithParityStatus(
        EncodingStatus.ParityStatus.REPAIR_ACTIVE.ordinal());
  }

  @Override
  public int countActiveParityRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, PARITY_STATUS + "=" +
        EncodingStatus.ParityStatus.REPAIR_ACTIVE.ordinal());
  }

  @Override
  public void setLostBlockCount(int n) {

  }

  @Override
  public int getLostBlockCount() {
    return 0;
  }

  @Override
  public void setLostParityBlockCount(int n) {

  }

  @Override
  public int getLostParityBlockCount() {
    return 0;
  }

  @Override
  public Collection<EncodingStatus> findDeleted(int limit)
      throws StorageException {
    return findWithStatus(EncodingStatus.Status.DELETED.ordinal(), limit);
  }

  @Override
  public Collection<EncodingStatus> findRevoked() throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    HopsQueryBuilder builder = session.getQueryBuilder();
    HopsQueryDomainType<EncodingStatusDto> domain =
        builder.createQueryDefinition(EncodingStatusDto.class);
    domain.where(domain.get("revoked").equal(domain.param(REVOKED)));
    HopsQuery<EncodingStatusDto> query = session.createQuery(domain);
    query.setParameter(REVOKED, NdbBoolean.convert(true));

    List<EncodingStatusDto> results = query.getResultList();
    List<EncodingStatus> esl = createHopEncodings(results);
    session.release(results);
    return esl;
  }

  private List<EncodingStatus> createHopEncodings(
      List<EncodingStatusDto> list) {
    List<EncodingStatus> result = new ArrayList<>(list.size());
    for (EncodingStatusDto dto : list) {
      result.add(createHopEncoding(dto));
    }
    return result;
  }

  private List<EncodingStatus> createHopEncodingsIfFound(
      List<EncodingStatusDto> list) {
    List<EncodingStatus> result = new ArrayList<>(list.size());
    for (EncodingStatusDto dto : list) {
      if(dto.getStatus()!=NOT_FOUND){
        result.add(createHopEncoding(dto));
      }
    }
    return result;
  }
  
  private EncodingStatus createHopEncoding(EncodingStatusDto dto) {
    if (dto == null) {
      return null;
    }

    // This is necessary because Integer cannot be used for clusterj fields with an index.
    // But it shouldn't be 0 anyway as it is always the root folder
    Long parityInodeId = null;
    if (dto.getParityInodeId() != 0) {
      parityInodeId = dto.getParityInodeId();
    }

    EncodingPolicy policy =
        new EncodingPolicy(dto.getCodec(), dto.getTargetReplication());
    EncodingStatus.Status status = dto.getStatus() == null ? null :
        EncodingStatus.Status.values()[dto.getStatus()];
    EncodingStatus.ParityStatus parityStatus =
        dto.getParityStatus() == null ? null :
            EncodingStatus.ParityStatus.values()[dto.getParityStatus()];
    return new EncodingStatus(dto.getInodeId(), parityInodeId, status,
        parityStatus, policy, dto.getStatusModificationTime(),
        dto.getParityStatusModificationTime(), dto.getParityFileName(),
        dto.getLostBlockCount(), dto.getLostParityBlockCount(),
        NdbBoolean.convert(dto.getRevoked()));
  }

  private List<EncodingStatus> findAllWithStatus(int status)
      throws StorageException {
    return findWithStatus(status, Long.MAX_VALUE);
  }

  private List<EncodingStatus> findAllWithParityStatus(int status)
      throws StorageException {
    return findWithParityStatus(status, Long.MAX_VALUE);
  }

  private static final String STATUS_QUERY =
      "SELECT * FROM %s WHERE %s=%s ORDER BY %s ASC LIMIT %s";

  private List<EncodingStatus> findWithParityStatus(int findStatus, long limit)
      throws StorageException {
    String query = String
        .format(STATUS_QUERY, TABLE_NAME, PARITY_STATUS, findStatus,
            PARITY_STATUS_MODIFICATION_TIME, limit);
    return find(query);
  }

  private List<EncodingStatus> findWithStatus(int findStatus, long limit)
      throws StorageException {
    String query = String.format(STATUS_QUERY, TABLE_NAME, STATUS, findStatus,
        STATUS_MODIFICATION_TIME, limit);
    return find(query);
  }

  private List<EncodingStatus> find(String query) throws StorageException {
    ArrayList<EncodingStatus> resultList;
    PreparedStatement s = null;
    ResultSet result = null;
    try {
      Connection conn = mysqlConnector.obtainSession();
      s = conn.prepareStatement(query);
      result = s.executeQuery();

      resultList = new ArrayList<>();

      while (result.next()) {
        Long inodeId = result.getLong(INODE_ID);
        Long parityInodeId = result.getLong(PARITY_INODE_ID);
        Integer status = result.getInt(STATUS);
        String codec = result.getString(CODEC);
        Short targetReplication = result.getShort(TARGET_REPLICATION);
        Long statusModificationTime = result.getLong(STATUS_MODIFICATION_TIME);
        Integer parityStatus = result.getInt(PARITY_STATUS);
        Long parityStatusModificationTime = result.getLong(PARITY_STATUS_MODIFICATION_TIME);
        String parityFileName = result.getString(PARITY_FILE_NAME);
        int lostBlocks = result.getInt(LOST_BLOCKS);
        int lostParityBlocks = result.getInt(LOST_PARITY_BLOCKS);
        Boolean revoked = NdbBoolean.convert(result.getByte(REVOKED));

        EncodingPolicy policy = new EncodingPolicy(codec, targetReplication);
        resultList.add(new EncodingStatus(inodeId, parityInodeId,
            EncodingStatus.Status.values()[status],
            EncodingStatus.ParityStatus.values()[parityStatus], policy,
            statusModificationTime, parityStatusModificationTime,
            parityFileName, lostBlocks, lostParityBlocks, revoked));
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
          LOG.warn("Exception when closing the ResultSet", ex);
        }
      }
      mysqlConnector.closeSession();
    }
    return resultList;
  }
}
