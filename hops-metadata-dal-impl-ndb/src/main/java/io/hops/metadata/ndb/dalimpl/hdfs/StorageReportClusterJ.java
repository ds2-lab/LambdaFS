package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.StorageReportDataAccess;
import io.hops.metadata.hdfs.entity.StorageReport;

import java.util.List;

public class StorageReportClusterJ
        implements TablesDef.StorageReportsTableDef, StorageReportDataAccess<StorageReport> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface StorageReportDTO {
        @PrimaryKey
        @Column(name = GROUP_ID)
        int getGroupId();
        void setGroupId(int groupId);

        @PrimaryKey
        @Column(name = REPORT_ID)
        int getReportId();
        void setReportId();

        @Column(name = FAILED)
        int getFailed();
        void setFailed(int failed);

        @Column(name = CAPACITY)
        long getCapacity();
        void setCapacity(long capacity);

        @Column(name = DFS_USED)
        long getDfsUsed();
        void setDfsUsed(long dfsUsed);

        @Column(name = REMAINING)
        long getRemaining();
        void setRemaining(long remaining);

        @Column(name = BLOCK_POOL_USED)
        long getBlockPoolUsed();
        void setBlockPoolUsed(long blockPoolUsed);

        @Column(name = DATANODE_STORAGE_ID)
        String getDatanodeStorageId();
        void setDatanodeStorageId(String datanodeStorageId);
    }

    @Override
    public StorageReport getStorageReport(int groupId, int reportId) throws StorageException {
        return null;
    }

    @Override
    public void removeStorageReport(int groupId, int reportId) throws StorageException {

    }

    @Override
    public void removeStorageReports(int groupId) throws StorageException {

    }

    @Override
    public void addStorageReport(StorageReport storageReport) throws StorageException {

    }

    @Override
    public List<StorageReport> getStorageReports(int groupId) throws StorageException {
        return null;
    }
}
