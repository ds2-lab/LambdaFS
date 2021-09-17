/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2019  hops.io
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
import io.hops.metadata.hdfs.dal.FileProvXAttrBufferDataAccess;
import io.hops.metadata.hdfs.entity.FileProvXAttrBufferEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileProvXAttrBufferClusterj implements TablesDef.FileProvXAttrBufferTableDef,
  FileProvXAttrBufferDataAccess<FileProvXAttrBufferEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface FileProvXAttrBufferEntryDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeId);

    @PrimaryKey
    @Column(name = NAMESPACE)
    byte getNamespace();

    void setNamespace(byte id);

    @PrimaryKey
    @Column(name = NAME)
    String getName();

    void setName(String name);
    
    @PrimaryKey
    @Column(name = INODE_LOGICAL_TIME)
    int getINodeLogicalTime();

    void setINodeLogicalTime(int inodeLogicalTime);
  
    @PrimaryKey
    @Column(name = INDEX)
    short getIndex();
  
    void setIndex(short index);
  
    @Column(name = NUM_PARTS)
    short getNumParts();
  
    void setNumParts(short numParts);
    
    @Column(name = VALUE)
    byte[] getValue();

    void setValue(byte[] value);
  }

  private List<FileProvXAttrBufferEntryDto> createPersistable(FileProvXAttrBufferEntry logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FileProvXAttrBufferEntryDto> dtos = new ArrayList<>();
    short numParts = logEntry.getNumParts();
    for(short index = 0; index < numParts; index++){
      FileProvXAttrBufferEntryDto dto = session.newInstance(FileProvXAttrBufferEntryDto.class);
      dto.setINodeId(logEntry.getInodeId());
      dto.setNamespace(logEntry.getNamespace());
      dto.setName(logEntry.getName());
      dto.setINodeLogicalTime(logEntry.getINodeLogicalTime());
      dto.setValue(logEntry.getValue(index));
      dto.setIndex(index);
      dto.setNumParts(numParts);
      dtos.add(dto);
    }
    return dtos;
  }
  
  @Override
  public void add(FileProvXAttrBufferEntry logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FileProvXAttrBufferEntryDto> dtos = null;
    try {
      dtos = createPersistable(logEntry);
      session.savePersistentAll(dtos);
    } finally {
      session.release(dtos);
    }
  }
  
  @Override
  public void addAll(Collection<FileProvXAttrBufferEntry> logEntries)
    throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<FileProvXAttrBufferEntryDto> added = new ArrayList<>(logEntries.size());
    try {
      for (FileProvXAttrBufferEntry logEntry : logEntries) {
        added.addAll(createPersistable(logEntry));
      }
      session.savePersistentAll(added);
    } finally {
      session.release(added);
    }
  }
}
