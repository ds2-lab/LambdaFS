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
package io.hops.metadata.ndb.dalimpl.election;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.entity.LeDescriptor;

import java.security.InvalidParameterException;

public class HdfsLeaderClusterj extends LeDescriptorClusterj
    implements TablesDef.HdfsLeaderTableDef {

  @PersistenceCapable(table = TABLE_NAME)
  public interface HdfsLeaderDTO extends LeaderDTO {

    @PrimaryKey
    @Column(name = ID)
    @Override
    long getId();

    @Override
    void setId(long id);

    @PrimaryKey
    @Column(name = PARTITION_VAL)
    @Override
    int getPartitionVal();

    @Override
    void setPartitionVal(int partitionVal);

    @Column(name = COUNTER)
    @Override
    long getCounter();

    @Override
    void setCounter(long counter);

    @Column(name = RPC_ADDRESSES)
    @Override
    String getHostname();

    @Override
    void setHostname(String hostname);

    @Column(name = HTTP_ADDRESS)
    @Override
    String getHttpAddress();

    @Override
    void setHttpAddress(String httpAddress);
    
    @Column(name = LOCATION_DOMAIN_ID)
    @Override
    byte getLocationDomainId();
    
    @Override
    void setLocationDomainId(byte domainId);
  }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new LeDescriptor.HdfsLeDescriptor(lTable.getId(),
        lTable.getCounter(), lTable.getHostname(), lTable.getHttpAddress(),
        lTable.getLocationDomainId());
  }

  public HdfsLeaderClusterj() {
    super(HdfsLeaderDTO.class);
  }
}
