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
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SafeBlocksClusterj
    implements TablesDef.SafeBlocksTableDef, SafeBlocksDataAccess {
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface SafeBlockDTO {
    
    @PrimaryKey
    @Column(name = ID)
    long getId();
    
    void setId(long id);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @Override
  public void insert(Collection<Long> safeBlocks) throws StorageException {
    final List<SafeBlockDTO> dtos =
        new ArrayList<>(safeBlocks.size());
    final HopsSession session = connector.obtainSession();
    try {
      for (Long blk : safeBlocks) {
        SafeBlockDTO dto = create(session, blk);
        dtos.add(dto);
      }
      session.savePersistentAll(dtos);
    }finally {
      session.release(dtos);
    }
  }
  
  @Override
  public boolean isSafe(Long BlockId) throws StorageException {
    HopsSession session = connector.obtainSession();
    SafeBlockDTO dto = null;
    try {
      dto = session.find(SafeBlockDTO.class, BlockId);
      return dto!=null;
    }finally {
      session.release(dto);
    }
  }
  
  @Override
  public void remove(Long safeBlock) throws StorageException {
    HopsSession session = connector.obtainSession();
    SafeBlockDTO dto = null;
    try {
      dto = create(session, safeBlock);
      session.remove(dto);
    }finally {
      session.release(dto);
    }
  }


  @Override
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

  private SafeBlockDTO create(HopsSession session, Long blk)
      throws StorageException {
    SafeBlockDTO dto = session.newInstance(SafeBlockDTO.class);
    dto.setId(blk);
    return dto;
  }
}
