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

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.ndb.dalimpl.configurationstore.ConfClusterJ;
import io.hops.metadata.ndb.dalimpl.configurationstore.ConfMutationClusterJ;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.*;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsTransaction;
import io.hops.metadata.yarn.dal.AppProvenanceDataAccess;
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Properties;
import io.hops.metadata.yarn.dal.ReservationStateDataAccess;

public class ClusterjConnector implements StorageConnector<DBSession> {

  private final static ClusterjConnector instance = new ClusterjConnector();
  private static boolean isInitialized = false;
  private DBSessionProvider dbSessionProvider = null;
  static ThreadLocal<DBSession> sessions = new ThreadLocal<>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);
  private String clusterConnectString;
  private String databaseName;
  
  private ClusterjConnector() {
  }

  public static ClusterjConnector getInstance() {
    return instance;
  }

  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    if (isInitialized) {
      LOG.warn("SessionFactory is already initialized");
      return;
    }
    
    clusterConnectString = (String) conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING);
    LOG.info("Database connect string: " +
        conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    databaseName = (String) conf.get(Constants.PROPERTY_CLUSTER_DATABASE);
    LOG.info("Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    LOG.info("Max Transactions: " +
        conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));

    int initialPoolSize =
        Integer.parseInt((String) conf.get("io.hops.session.pool.size"));
    int reuseCount =
        Integer.parseInt((String) conf.get("io.hops.session.reuse.count"));
    dbSessionProvider =
        new DBSessionProvider(conf, reuseCount, initialPoolSize);
    
    isInitialized = true;
  }

  /*
   * Return a dbSession from a random dbSession factory in our pool.
   *
   * NOTE: Do not close the dbSession returned by this call or you will die.
   */
  @Override
  public HopsSession obtainSession() throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession == null) {
      dbSession = dbSessionProvider.getSession();
      sessions.set(dbSession);
    }
    return dbSession.getSession();
  }

  @Override
  public void returnSession(boolean error) throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession != null) {
      sessions.remove(); // remove, and return to the pool
      dbSessionProvider.returnSession(dbSession,
          error); // if there was an error then close the session
    }
  }

  /**
   * begin a transaction.
   *
   * @throws io.hops.exception.StorageException
   */
  @Override
  public void beginTransaction() throws StorageException {
    HopsSession session = obtainSession();
    if (session.currentTransaction().isActive()) {
      LOG.fatal("Prevented starting transaction within a transaction.");
      throw new Error("Can not start Tx inside another Tx");
    }
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   *
   * @throws io.hops.exception.StorageException
   */
  @Override
  public void commit() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (!tx.isActive()) {
        throw new StorageException("The transaction is not began!");
      }
      tx.commit();
    } catch (StorageException e) {
      dbError = true;
      throw e;
    } finally {
      returnSession(dbError);
    }
  }
 
  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (StorageException e) {
      dbError = true;
      throw e;
    } finally {
      returnSession(dbError);
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {
    return formatAll(true);
  }

  @Override
  public boolean formatYarnStorage() throws StorageException {
    return formatYarn(true);
  }

  @Override
  public boolean formatHDFSStorage() throws StorageException {
    return formatHDFS(true);
  }

  @Override
  public boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException {
    return format(true, das);
  }


  @Override
  public boolean isTransactionActive() throws StorageException {
    return obtainSession().currentTransaction().isActive();
  }

  @Override
  public void stopStorage() throws StorageException {
    dbSessionProvider.stop();
  }

  @Override
  public void readLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.READ_COMMITTED);
  }

  @Override
  public void setPartitionKey(Class className, Object key)
      throws StorageException {
    Class cls = null;
    if (className == BlockInfoDataAccess.class) {
      cls = BlockInfoClusterj.BlockInfoDTO.class;
    } else if (className == PendingBlockDataAccess.class) {
      cls = PendingBlockClusterj.PendingBlockDTO.class;
    } else if (className == ReplicaUnderConstructionDataAccess.class) {
      cls = ReplicaUnderConstructionClusterj.ReplicaUcDTO.class;
    } else if (className == INodeDataAccess.class) {
      cls = INodeClusterj.InodeDTO.class;
    } else if (className == DirectoryWithQuotaFeatureDataAccess.class) {
      cls = DirectoryWithQuotaFeatureClusterj.INodeAttributesDTO.class;
    } else if (className == LeaseDataAccess.class) {
      cls = LeaseClusterj.LeaseDTO.class;
    } else if (className == LeasePathDataAccess.class) {
      cls = LeasePathClusterj.LeasePathsDTO.class;
    } else if (className == HdfsLeDescriptorDataAccess.class) {
      cls = HdfsLeaderClusterj.HdfsLeaderDTO.class;
    } else if (className == YarnLeDescriptorDataAccess.class) {
      cls = YarnLeaderClusterj.YarnLeaderDTO.class;
    } else if (className == ReplicaDataAccess.class) {
      cls = ReplicaClusterj.ReplicaDTO.class;
    } else if (className == CorruptReplicaDataAccess.class) {
      cls = CorruptReplicaClusterj.CorruptReplicaDTO.class;
    } else if (className == ExcessReplicaDataAccess.class) {
      cls = ExcessReplicaClusterj.ExcessReplicaDTO.class;
    } else if (className == InvalidateBlockDataAccess.class) {
      cls = InvalidatedBlockClusterj.InvalidateBlocksDTO.class;
    } else if (className == UnderReplicatedBlockDataAccess.class) {
      cls = UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class;
    } else if (className == VariableDataAccess.class) {
      cls = VariableClusterj.VariableDTO.class;
    } else if (className == QuotaUpdateDataAccess.class) {
      cls = QuotaUpdateClusterj.QuotaUpdateDTO.class;
    } else if (className == EncodingStatusDataAccess.class) {
      cls = EncodingStatusClusterj.EncodingStatusDto.class;
    } else if (className == BlockChecksumDataAccess.class) {
      cls = BlockChecksumClusterj.BlockChecksumDto.class;
    } else if (className == OngoingSubTreeOpsDataAccess.class) {
      cls = OnGoingSubTreeOpsClusterj.OnGoingSubTreeOpsDTO.class;
    } else if (className == HashBucketDataAccess.class){
      cls = HashBucketClusterj.HashBucketDTO.class;
    } else if (className == InMemoryInodeDataAccess.class) {
      cls = InMemoryFileInodeClusterj.FileInodeDataDTO.class;
    } else if (className == SmallOnDiskInodeDataAccess.class) {
      cls = SmallOnDiskFileInodeClusterj.FileInodeDataDTO.class;
    } else if (className == MediumOnDiskInodeDataAccess.class) {
      cls = MediumOnDiskFileInodeClusterj.FileInodeDataDTO.class;
    } else if (className == LargeOnDiskInodeDataAccess.class) {
      cls = LargeOnDiskFileInodeClusterj.FileInodeDataDTO.class;
    } else if (className == AceDataAccess.class){
      cls = AceClusterJ.AceDto.class;
    } else if (className == RetryCacheEntryDataAccess.class){
      cls = RetryCacheEntryClusterj.RetryCacheEntryDTO.class;
    } else if (className == CacheDirectiveDataAccess.class){
      cls = CacheDirectiveClusterj.CacheDirectiveDTO.class;
    } else if (className == CachePoolDataAccess.class){
      cls = CachePoolClusterJ.CachePoolDTO.class;
    } else if (className == CachedBlockDataAccess.class){
      cls = CachedBlockClusterJ.CachedBlockDTO.class;
    } else if (className == ActiveBlockReportsClusterj.class){
      cls = ActiveBlockReportsClusterj.class;
    } else if (className == ConfMutationDataAccess.class) {
      cls = ConfMutationClusterJ.ConfMutationDTO.class;
    } else if (className == ConfDataAccess.class) {
      cls = ConfClusterJ.ConfDTO.class;
    } else if (className == LeaseCreationLocksDataAccess.class) {
      cls = LeaseCreationLocksClusterj.LeaseCreationLockDTO.class;
    }

    HopsSession session = obtainSession();
    session.setPartitionKey(cls, key);
    session.flush();
  }

    @Override
  public boolean formatAllStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatYarnStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatHDFSStorageNonTransactional() throws StorageException {
    return formatHDFS(false);
  }

  private boolean formatYarn(boolean transactional) throws StorageException{
    return format(transactional, YarnLeDescriptorDataAccess.class, ApplicationStateDataAccess.class,
        ApplicationAttemptStateDataAccess.class, DelegationKeyDataAccess.class, DelegationTokenDataAccess.class,
        ProjectQuotaDataAccess.class, ProjectsDailyCostDataAccess.class, PriceMultiplicatorDataAccess.class,
        ReservationStateDataAccess.class, ConfMutationDataAccess.class, ConfDataAccess.class, 
        AppProvenanceDataAccess.class);
  }
  
  private boolean formatHDFS(boolean transactional) throws StorageException{
    return format(transactional,
        INodeDataAccess.class, InMemoryInodeDataAccess.class,
        SmallOnDiskInodeDataAccess.class, MediumOnDiskInodeDataAccess.class,
        LargeOnDiskInodeDataAccess.class,
        BlockInfoDataAccess.class, LeaseDataAccess.class,
        LeasePathDataAccess.class, ReplicaDataAccess.class,
        ReplicaUnderConstructionDataAccess.class,
        InvalidateBlockDataAccess.class, ExcessReplicaDataAccess.class,
        PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
        UnderReplicatedBlockDataAccess.class, HdfsLeDescriptorDataAccess.class,
        DirectoryWithQuotaFeatureDataAccess.class, StorageIdMapDataAccess.class,
        BlockLookUpDataAccess.class, SafeBlocksDataAccess.class,
        MisReplicatedRangeQueueDataAccess.class, QuotaUpdateDataAccess.class,
        EncodingStatusDataAccess.class, BlockChecksumDataAccess.class,
        OngoingSubTreeOpsDataAccess.class,
        MetadataLogDataAccess.class, EncodingJobsDataAccess.class,
        RepairJobsDataAccess.class, UserDataAccess.class, GroupDataAccess.class,
        UserGroupDataAccess.class,VariableDataAccess.class,
        HashBucketDataAccess.class, StorageDataAccess.class,
        AceDataAccess.class, RetryCacheEntryDataAccess.class, CacheDirectiveDataAccess.class,
        CachePoolDataAccess.class, CachedBlockDataAccess.class,
        ActiveBlockReportsDataAccess.class, XAttrDataAccess.class, EncryptionZoneDataAccess.class,
        FileProvenanceDataAccess.class, FileProvXAttrBufferDataAccess.class,
        LeaseCreationLocksDataAccess.class);
  }
  
  private boolean formatAll(boolean transactional) throws StorageException {
    //HDFS
    if (!formatHDFS(transactional)) {
      return false;
    }
    //YARN
    if (!formatYarn(transactional)) {
      return false;
    }

    // shared
    return format(transactional,
            VariableDataAccess.class
    );
  }

  private boolean format(boolean transactional,
      Class<? extends EntityDataAccess>... das) throws StorageException {
    
    final int RETRIES = 5; // in test
    for (int i = 0; i < RETRIES; i++) {
      try {
        for (Class e : das) {
          if (e == StorageDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.StoragesTableDef.TABLE_NAME);
          } else if (e == INodeDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.INodeTableDef.TABLE_NAME);
          } else if(e == InMemoryInodeDataAccess.class){
            MysqlServerConnector.truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.FileInodeInMemoryData.TABLE_NAME);
          } else if(e == SmallOnDiskInodeDataAccess.class){
            MysqlServerConnector.truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.FileInodeSmallDiskData.TABLE_NAME);
          } else if(e == MediumOnDiskInodeDataAccess.class){
            MysqlServerConnector.truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.FileInodeMediumlDiskData.TABLE_NAME);
          } else if(e == LargeOnDiskInodeDataAccess.class){
            MysqlServerConnector.truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.FileInodeLargeDiskData.TABLE_NAME);
          } else if (e == BlockInfoDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockInfoTableDef.TABLE_NAME);
          } else if (e == LeaseDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeaseTableDef.TABLE_NAME);
          } else if (e == LeasePathDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeasePathTableDef.TABLE_NAME);
          } else if (e == OngoingSubTreeOpsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.OnGoingSubTreeOpsDef.TABLE_NAME);
          } else if (e == ReplicaDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ReplicaTableDef.TABLE_NAME);
          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.ReplicaUnderConstructionTableDef.TABLE_NAME);
          } else if (e == InvalidateBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.InvalidatedBlockTableDef.TABLE_NAME);
          } else if (e == ExcessReplicaDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ExcessReplicaTableDef.TABLE_NAME);
          } else if (e == PendingBlockDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.PendingBlockTableDef.TABLE_NAME);
          } else if (e == CorruptReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.CorruptReplicaTableDef.TABLE_NAME);
          } else if (e == UnderReplicatedBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.UnderReplicatedBlockTableDef.TABLE_NAME);
          } else if (e == HdfsLeDescriptorDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, TablesDef.HdfsLeaderTableDef.TABLE_NAME);
          } else if (e == DirectoryWithQuotaFeatureDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.DirectoryWithQuotaFeatureTableDef.TABLE_NAME);
          } else if (e == VariableDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (Variable.Finder varType : Variable.Finder.values()) {
              LOG.debug("write varialbe " + varType.name() + " with id " + varType.getId());
              VariableClusterj.VariableDTO vd =
                  session.newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              byte[] value = varType.getDefaultValue();
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
          } else if (e == StorageIdMapDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.StorageIdMapTableDef.TABLE_NAME);
          } else if (e == BlockLookUpDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockLookUpTableDef.TABLE_NAME);
          } else if (e == SafeBlocksDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.SafeBlocksTableDef.TABLE_NAME);
          } else if (e == MisReplicatedRangeQueueDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MisReplicatedRangeQueueTableDef.TABLE_NAME);
          } else if (e == QuotaUpdateDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.QuotaUpdateTableDef.TABLE_NAME);
          } else if (e == EncodingStatusDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.EncodingStatusTableDef.TABLE_NAME);
          } else if (e == BlockChecksumDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockChecksumTableDef.TABLE_NAME);
          } else if (e == MetadataLogDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.TABLE_NAME);
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.LOOKUP_TABLE_NAME);
          } else if (e == EncodingJobsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.EncodingJobsTableDef.TABLE_NAME);
          } else if (e == RepairJobsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.RepairJobsTableDef.TABLE_NAME);
          } else if (e == UserDataAccess.class) {
            // Truncate not supported with FK
            MysqlServerConnector
                .truncateTable(true, io.hops.metadata.hdfs.TablesDef.UsersTableDef.TABLE_NAME);
          }else if (e == GroupDataAccess.class) {
            // Truncate not supported with FK
            MysqlServerConnector
                .truncateTable(true, io.hops.metadata.hdfs.TablesDef.GroupsTableDef.TABLE_NAME);
          }else if (e == UserGroupDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.UsersGroupsTableDef.TABLE_NAME);
          } else if (e == YarnLeDescriptorDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional,
                    TablesDef.YarnLeaderTableDef.TABLE_NAME);
          } else if (e == ApplicationAttemptStateDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ApplicationAttemptStateTableDef.TABLE_NAME);
          } else if (e == ApplicationStateDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ApplicationStateTableDef.TABLE_NAME);
          } else if (e == DelegationKeyDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.DelegationKeyTableDef.TABLE_NAME);
          } else if (e == DelegationTokenDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.DelegationTokenTableDef.TABLE_NAME);
          } else if (e == ProjectQuotaDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ProjectQuotaTableDef.TABLE_NAME);
          } else if (e == ProjectsDailyCostDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ProjectsDailyCostTableDef.TABLE_NAME);
          } else if (e == PriceMultiplicatorDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.PriceMultiplicatorTableDef.TABLE_NAME);
          } else if (e == HashBucketDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef
                .HashBucketsTableDef.TABLE_NAME);
          } else if (e == ReservationStateDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ReservationStateTableDef.TABLE_NAME);
          } else if (e == AceDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.AcesTableDef.TABLE_NAME);
          } else if (e == RetryCacheEntryDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.RetryCacheEntryTableDef.TABLE_NAME);
          } else if (e == CacheDirectiveDataAccess.class){
            // Truncate not supported with FK
            truncate(true, io.hops.metadata.hdfs.TablesDef.CacheDirectiveTableDef.TABLE_NAME);
          } else if (e == CachePoolDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.CachePoolTableDef.TABLE_NAME);
          } else if (e == CachedBlockDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.CachedBlockTableDef.TABLE_NAME);
          } else if (e == ActiveBlockReportsDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.ActiveBlockReports.TABLE_NAME);
          } else if (e == XAttrDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.XAttrTableDef.TABLE_NAME);
          } else if (e == ConfMutationDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ConfMutationTableDef.TABLE_NAME);
          } else if (e == ConfDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ConfTableDef.TABLE_NAME);
          } else if (e == EncryptionZoneDataAccess.class){
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.EncryptionZones.TABLE_NAME);
          } else if (e == FileProvenanceDataAccess.class) {
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.FileProvenanceTableDef.TABLE_NAME);
          }  else if (e == FileProvXAttrBufferDataAccess.class) {
            truncate(transactional, io.hops.metadata.hdfs.TablesDef.FileProvXAttrBufferTableDef.TABLE_NAME);
          }
        }
        MysqlServerConnector.truncateTable(transactional,
            "hdfs_path_memcached");
        return true;

      } catch (SQLException ex) {
        LOG.error(ex.getMessage(), ex);
      }
    } // end retry loop
    return false;
  }
  
  private void truncate(boolean transactional, String tableName)
      throws StorageException, SQLException {
    MysqlServerConnector.truncateTable(transactional, tableName);
  }

  @Override
  public void flush() throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession == null) {
      dbSession = dbSessionProvider.getSession();
      sessions.set(dbSession);
    }
    dbSession.getSession().flush();
  }

  public String getClusterConnectString() {
    return clusterConnectString;
  }

  public String getDatabaseName() {
    return databaseName;
  }
  
}
