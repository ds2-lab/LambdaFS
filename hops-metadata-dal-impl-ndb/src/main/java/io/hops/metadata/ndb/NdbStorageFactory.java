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
package io.hops.metadata.ndb;

import io.hops.exception.StorageException;
import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.ndb.dalimpl.configurationstore.ConfClusterJ;
import io.hops.metadata.ndb.dalimpl.configurationstore.ConfMutationClusterJ;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.*;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ReservationStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.AppProvenanceClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.quota.PriceMultiplicatorClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.quota.ProjectQuotaClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.quota.ProjectsDailyCostClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.yarn.dal.AppProvenanceDataAccess;
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import io.hops.metadata.yarn.dal.ReservationStateDataAccess;

public class NdbStorageFactory implements DalStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap =
      new HashMap<>();

  @Override
  public void setConfiguration(Properties conf)
          throws StorageInitializtionException {
    try {
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      //ClusterJ dumps username and password in the exception
      throw new StorageInitializtionException("Error getting connection to cluster");
    }
  }

  private void initDataAccessMap() {
    dataAccessMap.put(StorageDataAccess.class, new StoragesClusterj());
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class,
            new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap
            .put(DirectoryWithQuotaFeatureDataAccess.class, new DirectoryWithQuotaFeatureClusterj());
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(OngoingSubTreeOpsDataAccess.class, new OnGoingSubTreeOpsClusterj());
    dataAccessMap.put(InMemoryInodeDataAccess.class, new InMemoryFileInodeClusterj());
    dataAccessMap.put(SmallOnDiskInodeDataAccess.class, new SmallOnDiskFileInodeClusterj());
    dataAccessMap.put(MediumOnDiskInodeDataAccess.class, new MediumOnDiskFileInodeClusterj());
    dataAccessMap.put(LargeOnDiskInodeDataAccess.class, new LargeOnDiskFileInodeClusterj());
    dataAccessMap
            .put(HdfsLeDescriptorDataAccess.class, new HdfsLeaderClusterj());
    dataAccessMap
            .put(YarnLeDescriptorDataAccess.class, new YarnLeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap
            .put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap
            .put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap
            .put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class,
            new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap
            .put(EncodingStatusDataAccess.class, new EncodingStatusClusterj() {
            });
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    dataAccessMap.put(SafeBlocksDataAccess.class, new SafeBlocksClusterj());
    dataAccessMap.put(MisReplicatedRangeQueueDataAccess.class,
            new MisReplicatedRangeQueueClusterj());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
    dataAccessMap
            .put(BlockChecksumDataAccess.class, new BlockChecksumClusterj());
    dataAccessMap.put(MetadataLogDataAccess.class, new MetadataLogClusterj());
    dataAccessMap.put(EncodingJobsDataAccess.class, new EncodingJobsClusterj());
    dataAccessMap.put(RepairJobsDataAccess.class, new RepairJobsClusterj());
    dataAccessMap.put(UserDataAccess.class, new UserClusterj());
    dataAccessMap.put(GroupDataAccess.class, new GroupClusterj());
    dataAccessMap.put(UserGroupDataAccess.class, new UserGroupClusterj());
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class, new ApplicationAttemptStateClusterJ());
    dataAccessMap.put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
    dataAccessMap.put(DelegationTokenDataAccess.class, new DelegationTokenClusterJ());
    dataAccessMap.put(DelegationKeyDataAccess.class, new DelegationKeyClusterJ());
    dataAccessMap.put(ProjectQuotaDataAccess.class, new ProjectQuotaClusterJ());
    dataAccessMap.put(ProjectsDailyCostDataAccess.class, new ProjectsDailyCostClusterJ());
    dataAccessMap.put(PriceMultiplicatorDataAccess.class,new PriceMultiplicatorClusterJ());
    dataAccessMap.put(ReservationStateDataAccess.class, new ReservationStateClusterJ());
    dataAccessMap.put(HashBucketDataAccess.class, new HashBucketClusterj());
    dataAccessMap.put(AceDataAccess.class, new AceClusterJ());
    dataAccessMap.put(RetryCacheEntryDataAccess.class, new RetryCacheEntryClusterj());
    dataAccessMap.put(CacheDirectiveDataAccess.class, new CacheDirectiveClusterj());
    dataAccessMap.put(CachePoolDataAccess.class, new CachePoolClusterJ());
    dataAccessMap.put(CachedBlockDataAccess.class, new CachedBlockClusterJ());
    dataAccessMap.put(ActiveBlockReportsDataAccess.class, new ActiveBlockReportsClusterj());
    dataAccessMap.put(XAttrDataAccess.class, new XAttrClusterJ());
    dataAccessMap.put(ConfMutationDataAccess.class, new ConfMutationClusterJ());
    dataAccessMap.put(ConfDataAccess.class, new ConfClusterJ());
    dataAccessMap.put(EncryptionZoneDataAccess.class, new EncryptionZoneClusterJ());
    dataAccessMap.put(FileProvenanceDataAccess.class, new FileProvenanceClusterj());
    dataAccessMap.put(AppProvenanceDataAccess.class, new AppProvenanceClusterJ());
    dataAccessMap.put(FileProvXAttrBufferDataAccess.class, new FileProvXAttrBufferClusterj());
    dataAccessMap.put(LeaseCreationLocksDataAccess.class, new LeaseCreationLocksClusterj());
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
  
  @Override
  public boolean hasResources(double threshold) throws StorageException {
    return MysqlServerConnector.hasResources(threshold);
  }
  
}
