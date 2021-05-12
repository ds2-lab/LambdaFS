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
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import static io.hops.metadata.yarn.TablesDef.AppProvenanceTableDef.SUBMIT_TIME;
import static io.hops.metadata.yarn.TablesDef.AppProvenanceTableDef.TIMESTAMP;
import io.hops.metadata.yarn.dal.AppProvenanceDataAccess;
import io.hops.metadata.yarn.entity.AppProvenanceEntry;
import java.util.ArrayList;
import java.util.Collection;

public class AppProvenanceClusterJ implements TablesDef.AppProvenanceTableDef,
  AppProvenanceDataAccess<AppProvenanceEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface AppProvenanceEntryDto {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = NAME)
    String getName();

    void setName(String name);
    
    @PrimaryKey
    @Column(name = STATE)
    String getState();

    void setState(String state);

    @Column(name = USER)
    String getUser();

    void setUser(String user);
    
    @PrimaryKey
    @Column(name = TIMESTAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);
    
    @PrimaryKey
    @Column(name = SUBMIT_TIME)
    long getSubmitTime();

    void setSubmitTime(long submitTime);
    
    @PrimaryKey
    @Column(name = START_TIME)
    long getStartTime();

    void setStartTime(long startTime);
    
    @PrimaryKey
    @Column(name = FINISH_TIME)
    long getFinishTime();

    void setFinishTime(long finishTime);
  }

  @Override
  public void addAll(Collection<AppProvenanceEntry> entries)
    throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<AppProvenanceEntryDto> added = new ArrayList<>(entries.size());
    try {
      for (AppProvenanceEntry entry : entries) {
        added.add(createPersistable(entry));
      }
      session.savePersistentAll(added);
    } finally {
      session.release(added);
    }
  }

  @Override
  public void add(AppProvenanceEntry entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    AppProvenanceEntryDto dto = null;
    try {
      dto = createPersistable(entry);
      session.savePersistent(dto);
    } finally {
      session.release(dto);
    }
  }

  private AppProvenanceEntryDto createPersistable(AppProvenanceEntry entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    AppProvenanceEntryDto dto = session.newInstance(AppProvenanceEntryDto.class);
    dto.setId(entry.getId());
    dto.setName(entry.getName());
    dto.setState(entry.getState());
    dto.setUser(entry.getUser());
    dto.setTimestamp(entry.getTimestamp());
    dto.setSubmitTime(entry.getSubmitTime());
    dto.setStartTime(entry.getStartTime());
    dto.setFinishTime(entry.getFinishTime());
    return dto;
  }
}