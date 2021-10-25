package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.IntermediateBlockReportDataAccess;
import io.hops.metadata.hdfs.entity.IntermediateBlockReport;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class IntermediateBlockReportClusterJ
        implements TablesDef.IntermediateBlockReportsTableDef, IntermediateBlockReportDataAccess<IntermediateBlockReport>
{
    public static final Log LOG = LogFactory.getLog(IntermediateBlockReportClusterJ.class);
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @PersistenceCapable(table = TABLE_NAME)
    public interface IntermediateBlockReportDTO {
        @PrimaryKey
        @Column(name = REPORT_ID)
        int getReportId();
        void setReportId(int reportId);

        @PrimaryKey
        @Column(name = DATANODE_UUID)
        String getDatanodeUuid();
        void setDatanodeUuid(String datanodeUuid);

        @Column(name = PUBLISHED_AT)
        long getPublishedAt();
        void setPublishedAt(long publishedAt);

        @Column(name = POOL_ID)
        String getPoolId();
        void setPoolId(String poolId);

        @Column(name = RECEIVED_AND_DELETED_BLOCKS)
        String getReceivedAndDeletedBlocks();
        void setReceivedAndDeletedBlocks(String receivedAndDeletedBlocks);
    }

    @Override
    public IntermediateBlockReport getReport(int reportId, String datanodeUuid) throws StorageException {
        LOG.debug("GET IntermediateStorageReport -- reportId: " + reportId + ", datanodeUuid: " + datanodeUuid);

        HopsSession session = connector.obtainSession();

        Object[] primaryKey = {reportId, datanodeUuid};
        IntermediateBlockReportDTO reportDTO = session.find(IntermediateBlockReportDTO.class, primaryKey);

        if (reportDTO == null)
            return null;

        return convert(reportDTO);
    }

    /**
     * Internal function used to retrieve all the IntermediateBlockReportDTO instances associated with a particular
     * DataNode identified by the given UUID.
     * @param session HopsSession instance used to query the database.
     * @param datanodeUuid The DataNode whose associated IntermediateBlockReportDTO are to be retrieved.
     * @return A list of all IntermediateBlockReportDTO instances associated with the specified DataNode.
     */
    private List<IntermediateBlockReportDTO> getIntermediateBlockReportDTOs(HopsSession session, String datanodeUuid)
        throws StorageException {
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<IntermediateBlockReportDTO> domainType =
                queryBuilder.createQueryDefinition(IntermediateBlockReportDTO.class);

        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(predicateDatanodeUuid);

        HopsQuery<IntermediateBlockReportDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        return query.getResultList();
    }

    @Override
    public List<IntermediateBlockReport> getReports(String datanodeUuid) throws StorageException {
        LOG.debug("GET IntermediateStorageReports -- datanodeUuid: " + datanodeUuid);

        List<IntermediateBlockReportDTO> storeReportDTOs = getIntermediateBlockReportDTOs(
                connector.obtainSession(), datanodeUuid);

        ArrayList<IntermediateBlockReport> resultList = new ArrayList<>();

        for (IntermediateBlockReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    @Override
    public int deleteReports(String datanodeUuid) throws StorageException {
        HopsSession session = connector.obtainSession();
        List<IntermediateBlockReportDTO> storageReportDTOs = getIntermediateBlockReportDTOs(session, datanodeUuid);

        session.deletePersistentAll(storageReportDTOs);
        session.release(storageReportDTOs);

        // Return the number of Storage Reports that were deleted.
        return storageReportDTOs.size();
    }
    
    @Override
    public List<IntermediateBlockReport> getReportsPublishedAfter(String datanodeUuid, long publishedAt) throws StorageException {
        LOG.debug("GET IntermediateBlockReport -- DN UUID: " + datanodeUuid + ", publishedAt: " +
                publishedAt);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<IntermediateBlockReportDTO> domainType =
                queryBuilder.createQueryDefinition(IntermediateBlockReportDTO.class);

        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        HopsPredicate predicatePublishedAt =
                domainType.get("publishedAt").greaterEqual(domainType.param("publishedAtParam"));
        domainType.where(predicateDatanodeUuid.and(predicatePublishedAt));

        HopsQuery<IntermediateBlockReportDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);
        query.setParameter("publishedAtParam", publishedAt);

        List<IntermediateBlockReportDTO> storeReportDTOs = query.getResultList();

        ArrayList<IntermediateBlockReport> resultList = new ArrayList<>();

        for (IntermediateBlockReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    @Override
    public List<IntermediateBlockReport> getReports(String datanodeUuid, int minimumReportId) throws StorageException {
        LOG.debug("GET IntermediateStorageReport -- minimumReportId: " + minimumReportId
                + ", datanodeUuid: " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<IntermediateBlockReportDTO> domainType =
                queryBuilder.createQueryDefinition(IntermediateBlockReportDTO.class);

        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        HopsPredicate predicateReportId =
                domainType.get("reportId").greaterEqual(domainType.param("minimumReportIdParam"));
        domainType.where(predicateDatanodeUuid.and(predicateReportId));

        HopsQuery<IntermediateBlockReportDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);
        query.setParameter("minimumReportIdParam", minimumReportId);

        List<IntermediateBlockReportDTO> storeReportDTOs = query.getResultList();

        ArrayList<IntermediateBlockReport> resultList = new ArrayList<>();

        for (IntermediateBlockReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    @Override

    public void addReport(int reportId, String datanodeUuid, long publishedAt,
                          String poolId, String receivedAndDeletedBlocks)
            throws StorageException {
        LOG.debug("ADD IntermediateStorageReport -- reportId: " + reportId + ", datanodeUuid: " + datanodeUuid
            + ", poolId: " + poolId + ", length of encoded blocks string: " + receivedAndDeletedBlocks.length());

        HopsSession session = connector.obtainSession();
        //LOG.debug("Obtained session...");
        IntermediateBlockReportDTO dtoObject = null;

        try {
            dtoObject = session.newInstance(IntermediateBlockReportDTO.class);
            //LOG.debug("Created instance of class IntermediateBlockReportDTO...");
            dtoObject.setReportId(reportId);
            dtoObject.setDatanodeUuid(datanodeUuid);
            dtoObject.setPublishedAt(publishedAt);
            dtoObject.setPoolId(poolId);
            dtoObject.setReceivedAndDeletedBlocks(receivedAndDeletedBlocks);

            session.savePersistent(dtoObject);
            //LOG.debug("Saved IntermediateBlockReportDTO instance to intermediate storage...");
        } finally {
            session.release(dtoObject);
            //LOG.debug("Released IntermediateBlockReportDTO instance...");
        }

    }

    private IntermediateBlockReport convert(IntermediateBlockReportDTO src) {
        return new IntermediateBlockReport(src.getReportId(), src.getDatanodeUuid(), src.getPublishedAt(),
                src.getPoolId(), src.getReceivedAndDeletedBlocks());
    }
}
