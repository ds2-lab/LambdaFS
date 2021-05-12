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
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.hdfs.entity.BlockLookUp;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockLookUpClusterj
    implements TablesDef.BlockLookUpTableDef, BlockLookUpDataAccess<BlockLookUp> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockLookUpDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long iNodeID);

  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static long NOT_FOUND_ROW = -1000L;

  @Override
  public void prepare(Collection<BlockLookUp> modified,
      Collection<BlockLookUp> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<BlockLookUpDTO> changes = new ArrayList<>();
    List<BlockLookUpDTO> deletions = new ArrayList<>();

    try {
      for (BlockLookUp block_lookup : removed) {
        BlockLookUpClusterj.BlockLookUpDTO bTable = session
                .newInstance(BlockLookUpClusterj.BlockLookUpDTO.class,
                        block_lookup.getBlockId());
        deletions.add(bTable);
      }

      for (BlockLookUp block_lookup : modified) {
        BlockLookUpClusterj.BlockLookUpDTO bTable =
                session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        createPersistable(block_lookup, bTable);
        changes.add(bTable);
      }

      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public BlockLookUp findByBlockId(long blockId) throws StorageException {
    HopsSession session = connector.obtainSession();
    BlockLookUpClusterj.BlockLookUpDTO lookup =
        session.find(BlockLookUpClusterj.BlockLookUpDTO.class, blockId);
    if (lookup == null) {
      return null;
    }
    BlockLookUp blu = createBlockInfo(lookup);
    session.release(lookup);
    return blu;
  }

  @Override
  public long[] findINodeIdsByBlockIds(final long[] blockIds)
      throws StorageException {
    final HopsSession session = connector.obtainSession();
    return readINodeIdsByBlockIds(session, blockIds);
  }

  protected static long[] readINodeIdsByBlockIds(final HopsSession session,
      final long[] blockIds) throws StorageException {
    final List<BlockLookUpDTO> bldtos = new ArrayList<>();
    final List<Long> inodeIds = new ArrayList<>();
    try {
      for (long blockId : blockIds) {
        BlockLookUpDTO bldto =
            session.newInstance(BlockLookUpDTO.class, blockId);
        bldto.setINodeId(NOT_FOUND_ROW);
        bldto = session.load(bldto);
        bldtos.add(bldto);
      }
      session.flush();
  
      for (BlockLookUpDTO bld : bldtos) {
        if (bld.getINodeId() != NOT_FOUND_ROW) {
          inodeIds.add(bld.getINodeId());
        } else {
          BlockLookUpDTO bldn =
              session.find(BlockLookUpDTO.class, bld.getBlockId());
          if (bldn != null) {
            //[M] BUG:
            //ClusterjConnector.LOG.error("xxx: Inode doesn't exists retries for " + bld.getBlockId() + " inodeId " + bld.getINodeId() + " at index " + i);
            inodeIds.add(bldn.getINodeId());
            session.release(bldn);
          } else {
            inodeIds.add(NOT_FOUND_ROW);
          }
        }
      }
      return Longs.toArray(inodeIds);
    }finally {
      session.release(bldtos);
    }
  }
  
  @Override
  public Map<Long, List<Long>> getINodeIdsForBlockIds(final long[] blockIds) throws StorageException {
    final HopsSession session = connector.obtainSession();
    final List<BlockLookUpDTO> bldtos = new ArrayList<>();
    final Map<Long, List<Long>> InodeToBlockIdsMap = new HashMap<>(blockIds.length);
    try {
      for (long blockId : blockIds) {
        BlockLookUpDTO bldto =
            session.newInstance(BlockLookUpDTO.class, blockId);
        bldto.setINodeId(NOT_FOUND_ROW);
        bldto = session.load(bldto);
        bldtos.add(bldto);
      }
      session.flush();
  
      for (BlockLookUpDTO bld : bldtos) {
        if (bld.getINodeId() != NOT_FOUND_ROW) {
          addBlockId(InodeToBlockIdsMap, bld);
        } else {
          BlockLookUpDTO bldn =
              session.find(BlockLookUpDTO.class, bld.getBlockId());
          if (bldn != null) {
            //[M] BUG:
            //ClusterjConnector.LOG.error("xxx: Inode doesn't exists retries for " + bld.getBlockId() + " inodeId " + bld.getINodeId() + " at index " + i);
            addBlockId(InodeToBlockIdsMap, bld);
            session.release(bldn);
          } 
        }
      }
      return InodeToBlockIdsMap;
    }finally {
      session.release(bldtos);
    }
  }
  
  private void addBlockId(Map<Long, List<Long>> map, BlockLookUpDTO bld){
    List<Long> blockIds = map.get(bld.getINodeId());
    if(blockIds==null){
      blockIds = new ArrayList<>();
      map.put(bld.getINodeId(), blockIds);
    }
    blockIds.add(bld.getBlockId());
  }
  
  protected static BlockLookUp createBlockInfo(
      BlockLookUpClusterj.BlockLookUpDTO dto) {
    BlockLookUp lookup = new BlockLookUp(dto.getBlockId(), dto.getINodeId());
    return lookup;
  }

  protected static void createPersistable(BlockLookUp lookup,
      BlockLookUpClusterj.BlockLookUpDTO persistable) {
    persistable.setBlockId(lookup.getBlockId());
    persistable.setINodeId(lookup.getInodeId());
  }
}
