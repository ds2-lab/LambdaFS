package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.StorageReportDataAccess;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
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
        void setReportId(int reportId);

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

    private static final Log LOG = LogFactory.getLog(StorageReportClusterJ.class);

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public StorageReport getStorageReport(int groupId, int reportId) throws StorageException {
        LOG.info("GET StorageReport groupId = " + groupId + ", reportId = " + reportId);

        HopsSession session = connector.obtainSession();

        Object[] primaryKey = {groupId, reportId};
        StorageReportDTO report = session.find(StorageReportDTO.class, primaryKey);

        if (report == null)
            return null;

        return convert(report); // Convert the StorageReportDTO to a StorageReport and return it.
    }

    @Override
    public void removeStorageReport(int groupId, int reportId) throws StorageException {
        LOG.info("REMOVE StorageReport groupId = " + groupId + ", reportId = " + reportId);

        HopsSession session = connector.obtainSession();
        Object[] primaryKey = {groupId, reportId};
        StorageReportDTO deleteMe = session.find(StorageReportDTO.class, primaryKey);
        session.deletePersistent(StorageReportDTO.class, deleteMe);

        LOG.debug("Successfully removed/deleted DatanodeStorage with groupId = " + groupId
            + ", reportId = " + reportId);
    }

    @Override
    public void removeStorageReports(int groupId) throws StorageException {
        LOG.info("REMOVE StorageReport group " + groupId);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<StorageReportDTO> domainType = queryBuilder.createQueryDefinition(StorageReportDTO.class);
        HopsPredicate predicate = domainType.get("groupId").equal(domainType.param("groupIdParam"));
        domainType.where(predicate);
        HopsQuery<StorageReportDTO> query = session.createQuery(domainType);
        query.setParameter("groupIdParam", groupId);
        List<StorageReportDTO> storeReportDTOs = query.getResultList();

        session.deletePersistentAll(storeReportDTOs);
        session.release(storeReportDTOs);
    }

    @Override
    public void addStorageReport(StorageReport storageReport) throws StorageException {
        LOG.info("ADD StorageReport " + storageReport.toString());

        StorageReportDTO dtoObject = null;
        HopsSession session = connector.obtainSession();

        try {
            dtoObject = session.newInstance(StorageReportDTO.class);
            copyState(dtoObject, storageReport);
            session.savePersistent(dtoObject);

            LOG.debug("Wrote/persisted StorageReport groupId = " + dtoObject.getGroupId() + ", reportId = "
                    + dtoObject.getReportId() + " to MySQL NDB storage.");
        } finally {
            session.release(dtoObject);
        }
    }

    @Override
    public List<StorageReport> getStorageReports(int groupId) throws StorageException {
        LOG.info("GET StorageReport group " + groupId);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<StorageReportDTO> domainType = queryBuilder.createQueryDefinition(StorageReportDTO.class);
        HopsPredicate predicate = domainType.get("groupId").equal(domainType.param("groupIdParam"));
        domainType.where(predicate);
        HopsQuery<StorageReportDTO> query = session.createQuery(domainType);
        query.setParameter("groupIdParam", groupId);
        List<StorageReportDTO> storeReportDTOs = query.getResultList();

        ArrayList<StorageReport> resultList = new ArrayList<>();

        for (StorageReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.StorageReport} instance to the given
     * {@link io.hops.metadata.ndb.dalimpl.hdfs.StorageReportClusterJ.StorageReportDTO} instance.
     * @param dest The DatanodeStorageDTO destination object.
     * @param src The DatanodeStorage source object.
     */
    private void copyState(StorageReportDTO dest, StorageReport src) {
        dest.setGroupId(src.getGroupId());
        dest.setReportId(src.getReportId());
        dest.setFailed(src.getFailed() ? 1 : 0);
        dest.setCapacity(src.getCapacity());
        dest.setDfsUsed(src.getDfsUsed());
        dest.setRemaining(src.getRemaining());
        dest.setBlockPoolUsed(src.getBlockPoolUsed());
        dest.setDatanodeStorageId(src.getDatanodeStorageId());
    }

    /**
     * Convert the given StorageReportDTO instance to a StorageReport instance.
     */
    private static StorageReport convert(StorageReportDTO src) {
        boolean failed = false;
        if (src.getFailed() >= 1)
            failed = true;

        return new StorageReport(src.getGroupId(), src.getReportId(), failed, src.getCapacity(),
                src.getDfsUsed(), src.getRemaining(), src.getBlockPoolUsed(), src.getDatanodeStorageId());
    }
}
