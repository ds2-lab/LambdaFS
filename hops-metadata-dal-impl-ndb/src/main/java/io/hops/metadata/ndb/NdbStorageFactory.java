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
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.WriteAcknowledgementClusterJ;
import io.hops.metadata.ndb.dalimpl.hdfs.invalidations.InvalidationClusterJ;
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
import java.util.Set;

import io.hops.metadata.yarn.dal.ReservationStateDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NdbStorageFactory implements DalStorageFactory {

  /**
   * Used to keep track of the version that is being used so I can know if something is out of date.
   *
   * Syntax:
   *  Major.Minor.Build.Revision
   */
  private static final String versionNumber = "0.3.1.0";

  private final Map<Class, EntityDataAccess> dataAccessMap =
      new HashMap<>();

  private static final Log LOG = LogFactory.getLog(NdbStorageFactory.class);

  /**
   * Default constructor.
   */
  public NdbStorageFactory() {
    System.out.println("NdbStorageFactory Version Number: " + versionNumber);
  }

  @Override
  public void setConfiguration(Properties conf)
          throws StorageInitializtionException {
    LOG.debug("NdbStorageFactory Version Number: " + versionNumber);

    try {
      LOG.debug("Preparing to set configuration for ClusterJConnector now...");
      ClusterjConnector.getInstance().setConfiguration(conf);
      LOG.debug("Successfully set configuration for ClusterJConnector. Next, setting configuration for " +
              "MySQLServerConnector now...");
      MysqlServerConnector.getInstance().setConfiguration(conf);
      LOG.debug("Successfully set configuration for MySQLServerConnector. Next, initializing " +
              "data access map now...");
      initDataAccessMap();
      LOG.debug("Successfully initialized data access map.");
    } catch (IOException ex) {
      LOG.error("Encountered IOException while establishing network connections to NDB cluster: ", ex);

      //ClusterJ dumps username and password in the exception
      throw new StorageInitializtionException("Error getting connection to cluster");
    }
  }

  private void initDataAccessMap() {
    // These are the classes that I've added for our Serverless NameNode.
    dataAccessMap.put(InvalidationDataAccess.class, new InvalidationClusterJ());
    dataAccessMap.put(WriteAcknowledgementDataAccess.class, new WriteAcknowledgementClusterJ());
    dataAccessMap.put(ServerlessNameNodeDataAccess.class, new ServerlessNameNodeClusterJ());
    dataAccessMap.put(DataNodeDataAccess.class, new DataNodeClusterJ());
    dataAccessMap.put(StorageReportDataAccess.class, new StorageReportClusterJ());
    dataAccessMap.put(DatanodeStorageDataAccess.class, new DatanodeStorageClusterJ());
    dataAccessMap.put(IntermediateBlockReportDataAccess.class, new IntermediateBlockReportClusterJ());

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

  @Override
  public float getResourceMemUtilization() throws StorageException {
    return MysqlServerConnector.getResourceMemUtilization();
  }

  @Override
  public Map<Class, EntityDataAccess> getDataAccessMap() {
    return dataAccessMap;
  }
  
}
