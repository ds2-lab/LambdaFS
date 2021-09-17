/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2020 Logical Clocks AB
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

import com.mysql.clusterj.annotation.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.LeaseCreationLocksDataAccess;
import io.hops.metadata.hdfs.entity.LeaseCreationLock;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LeaseCreationLocksClusterj implements TablesDef.LeaseCreationLocksTableDef,
        LeaseCreationLocksDataAccess<Object> {
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  public static final Log LOG = LogFactory.getLog(LeaseCreationLocksClusterj.class);

  private final int NON_EXISTANT_ROW = -1;

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeaseCreationLockDTO {
    @PrimaryKey
    @Column(name = ID)
    int getID();
    void setID(int ID);
  }


  @Override
  public LeaseCreationLock lock(int lockRow) throws StorageException {
    int readID = readRow(lockRow);
    if (readID == NON_EXISTANT_ROW) {
      throw new StorageException("Cluster misconfiguration. Lease creation lock row not found");
    }

    return new LeaseCreationLock(readID);
  }

  @Override
  public void createLockRows(int count) throws StorageException {
    HopsSession session = connector.obtainSession();
    for (int i = 0; i < count; i++) {
      int readID = readRow(i);
      if (readID == NON_EXISTANT_ROW) {
        LeaseCreationLockDTO dto = session.newInstance(LeaseCreationLockDTO.class);
        dto.setID(i);
        session.savePersistent(dto);
        session.release(dto);
        LOG.debug("Added lease creation lock row with ID: "+i);
      }
    }
  }

  private int readRow(int rowID) throws StorageException {
    HopsSession session = connector.obtainSession();
    LeaseCreationLockDTO dto = session.find(LeaseCreationLockDTO.class, rowID);

    int readID = NON_EXISTANT_ROW;
    if (dto != null) {
      readID = dto.getID();
      session.release(dto);
    }
    return readID;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(LeaseCreationLockDTO.class);
  }
}
