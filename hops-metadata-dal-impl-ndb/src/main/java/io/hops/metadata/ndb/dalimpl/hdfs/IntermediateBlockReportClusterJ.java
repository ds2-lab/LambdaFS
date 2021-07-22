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

        @Column(name = POOL_ID)
        String getPoolId();
        void setPoolId(String poolId);

        @Column(name = RECEIVED_AND_DELETED_BLOCKS)
        String getReceivedAndDeletedBlocks();
        void setReceivedAndDeletedBlocks(String receivedAndDeletedBlocks);
    }

    @Override
    public IntermediateBlockReport getReport(int reportId, String datanodeUuid) throws StorageException {
        LOG.info("GET IntermediateStorageReport -- reportId: " + reportId + ", datanodeUuid: " + datanodeUuid);

        HopsSession session = connector.obtainSession();

        Object[] primaryKey = {reportId, datanodeUuid};
        IntermediateBlockReportDTO reportDTO = session.find(IntermediateBlockReportDTO.class, primaryKey);

        if (reportDTO == null)
            return null;

        return convert(reportDTO);
    }

    @Override
    public List<IntermediateBlockReport> getReports(String datanodeUuid) throws StorageException {
        LOG.info("GET IntermediateStorageReports -- datanodeUuid: " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<IntermediateBlockReportDTO> domainType =
                queryBuilder.createQueryDefinition(IntermediateBlockReportDTO.class);

        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(predicateDatanodeUuid);

        HopsQuery<IntermediateBlockReportDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<IntermediateBlockReportDTO> storeReportDTOs = query.getResultList();

        ArrayList<IntermediateBlockReport> resultList = new ArrayList<>();

        for (IntermediateBlockReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    @Override
    public List<IntermediateBlockReport> getReports(String datanodeUuid, int minimumReportId) throws StorageException {
        LOG.info("GET IntermediateStorageReport -- minimumReportId: " + minimumReportId
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
    public void addReport(int reportId, String datanodeUuid, String poolId, String receivedAndDeletedBlocks) throws StorageException {
        LOG.info("ADD IntermediateStorageReport -- reportId: " + reportId + ", datanodeUuid: " + datanodeUuid
            + ", poolId: " + poolId + ", length of encoded blocks string: " + receivedAndDeletedBlocks.length());

        HopsSession session = connector.obtainSession();
        LOG.info("Obtained session...");
        IntermediateBlockReportDTO dtoObject = null;

        try {
            dtoObject = session.newInstance(IntermediateBlockReportDTO.class);
            LOG.info("Created instance of class IntermediateBlockReportDTO...");
            dtoObject.setReportId(reportId);
            dtoObject.setDatanodeUuid(datanodeUuid);
            dtoObject.setPoolId(poolId);
            dtoObject.setReceivedAndDeletedBlocks(receivedAndDeletedBlocks);

            session.savePersistent(dtoObject);
            LOG.info("Saved IntermediateBlockReportDTO instance to intermediate storage...");
        } finally {
            session.release(dtoObject);
            LOG.info("Released IntermediateBlockReportDTO instance...");
        }

    }

    private IntermediateBlockReport convert(IntermediateBlockReportDTO src) {
        return new IntermediateBlockReport(src.getReportId(), src.getDatanodeUuid(), src.getPoolId(),
                src.getReceivedAndDeletedBlocks());
    }
}
