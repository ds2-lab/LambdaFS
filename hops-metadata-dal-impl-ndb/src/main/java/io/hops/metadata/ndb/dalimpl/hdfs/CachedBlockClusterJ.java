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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.CachedBlockDataAccess;
import io.hops.metadata.hdfs.entity.CachedBlock;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CachedBlockClusterJ implements TablesDef.CachedBlockTableDef, CachedBlockDataAccess<CachedBlock> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface CachedBlockDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blockId);

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @PrimaryKey
    @Column(name = DATANODE_ID)
    String getDataNodeId();

    void setDataNodeId(String datanodeId);

    @Column(name = STATUS)
    String getStatus();

    void setStatus(String status);

    @Column(name = REPLICATION_AND_MARK)
    short getReplicationAndMark();

    void setReplicationAndMark(short replicationAndMark);

  }

  @Override
  public CachedBlock find(long blockId, long inodeId, String datanodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[3];
    pk[0] = blockId;
    pk[1] = inodeId;
    pk[2] = datanodeId;

    CachedBlockDTO result = session.find(CachedBlockDTO.class, pk);
    if (result != null) {
      CachedBlock retryCacheEntry = convert(result);
      session.release(result);
      return retryCacheEntry;
    } else {
      return null;
    }
  }

  @Override
  public List<CachedBlock> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachedBlockDTO> dobj = qb.createQueryDefinition(CachedBlockDTO.class);
    HopsQuery<CachedBlockDTO> query = session.createQuery(dobj);
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public List<CachedBlock> findCachedBlockById(long blockId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachedBlockDTO> dobj = qb.createQueryDefinition(CachedBlockDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockId"));
    HopsQuery<CachedBlockDTO> query = session.createQuery(dobj);
    query.setParameter("blockId", blockId);
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<CachedBlock> findCachedBlockByINodeId(long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachedBlockDTO> dobj = qb.createQueryDefinition(CachedBlockDTO.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeId"));
    dobj.where(pred1);
    HopsQuery<CachedBlockDTO> query = session.createQuery(dobj);
    query.setParameter("inodeId", inodeId);
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<CachedBlock> findCachedBlockByINodeIds(long[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachedBlockDTO> dobj = qb.createQueryDefinition(CachedBlockDTO.class);
    HopsPredicate pred1 = dobj.get("inodeId").in(dobj.param("inodeId"));
    dobj.where(pred1);
    HopsQuery<CachedBlockDTO> query = session.createQuery(dobj);
    query.setParameter("inodeId", Longs.asList(inodeIds));
    return convertAndRelease(session, query.getResultList());
  }

  @Override
  public List<CachedBlock> findByIds(long[] blockIds, long[] inodeIds, String datanodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<CachedBlock> blks = readCachedBlockBatch(session, inodeIds, blockIds, datanodeId);
    return blks;
  }

  @Override
  public List<CachedBlock> findCachedBlockByDatanodeId(String dataNodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CachedBlockDTO> dobj = qb.createQueryDefinition(CachedBlockDTO.class);
    HopsPredicate pred1 = dobj.get("dataNodeId").equal(dobj.param("dataNodeId"));
    dobj.where(pred1);
    HopsQuery<CachedBlockDTO> query = session.createQuery(dobj);
    query.setParameter("dataNodeId", dataNodeId);
    return convertAndRelease(session, query.getResultList());
  }
  
  protected static Log LOG = LogFactory.getLog(CachedBlockClusterJ.class);

  @Override
  public void prepare(Collection<CachedBlock> removed, Collection<CachedBlock> newed, Collection<CachedBlock> modified)
      throws StorageException {
    List<CachedBlockDTO> changes = new ArrayList<>();
    List<CachedBlockDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    try {
      for (CachedBlock cachedBlock : removed) {
        CachedBlockDTO newInstance = session.newInstance(CachedBlockDTO.class);
        createPersistable(cachedBlock, newInstance);
        deletions.add(newInstance);
      }

      for (CachedBlock cachedBlock : newed) {
        LOG.info("GAUTIER persist cached block " + cachedBlock.getBlockId());
        CachedBlockDTO newInstance = session.newInstance(CachedBlockDTO.class);
        createPersistable(cachedBlock, newInstance);
        changes.add(newInstance);
      }

      for (CachedBlock cachedBlock : modified) {
        LOG.info("GAUTIER persist cached block " + cachedBlock.getBlockId());
        CachedBlockDTO newInstance = session.newInstance(CachedBlockDTO.class);
        createPersistable(cachedBlock, newInstance);
        changes.add(newInstance);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private List<CachedBlock> readCachedBlockBatch(final HopsSession session, final long[] inodeIds, final long[] blockIds,
      String datanodeId) throws StorageException {
    final List<CachedBlockDTO> bdtos = new ArrayList<>();
    for (int i = 0; i < blockIds.length; i++) {
      Object[] pk = new Object[]{blockIds[i], inodeIds[i], datanodeId};
      CachedBlockDTO bdto = session.newInstance(CachedBlockDTO.class, pk);
      bdto = session.load(bdto);
      bdtos.add(bdto);
    }
    session.flush();
    List<CachedBlock> lbis = convertAndRelease(session, bdtos);
    return lbis;
  }

  private List<CachedBlock> convertAndRelease(HopsSession session, List<CachedBlockDTO> dtos) throws StorageException {
    List<CachedBlock> cachedBlocks = new ArrayList<>(dtos.size());
    for (CachedBlockDTO dto : dtos) {
      cachedBlocks.add(convert(dto));
      session.release(dto);
    }
    return cachedBlocks;
  }

  private CachedBlock convert(CachedBlockDTO dto) {

    return new CachedBlock(dto.getBlockId(), dto.getInodeId(), dto.getDataNodeId(), dto.getStatus(), dto.
        getReplicationAndMark());
  }

  private void createPersistable(CachedBlock cachedBlock, CachedBlockDTO newInstance) {
    newInstance.setBlockId(cachedBlock.getBlockId());
    newInstance.setInodeId(cachedBlock.getInodeId());
    newInstance.setDataNodeId(cachedBlock.getDatanodeId());
    newInstance.setStatus(cachedBlock.getStatus());
    newInstance.setReplicationAndMark(cachedBlock.getReplicationAndMark());
  }
}
