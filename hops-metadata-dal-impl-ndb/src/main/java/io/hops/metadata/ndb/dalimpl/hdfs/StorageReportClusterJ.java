package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.StorageReportDataAccess;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StorageReportClusterJ
        implements TablesDef.StorageReportsTableDef, StorageReportDataAccess<StorageReport> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface StorageReportDTO {
        @PrimaryKey
        @Column(name = GROUP_ID)
        long getGroupId();
        void setGroupId(long groupId);

        @PrimaryKey
        @Column(name = REPORT_ID)
        int getReportId();
        void setReportId(int reportId);

        @PrimaryKey
        @Column(name = DATANODE_UUID)
        String getDatanodeUuid();
        void setDatanodeUuid(String datanodeUuid);

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
    private final MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();

    /**
     * Source: https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column
     *
     * This is the second method described by the top answer. That is, Left Joining with self,
     * tweaking join conditions and filters.
     *
     * When formatting this String, the parameters to String.format() should be as follows:
     *
     * String.format(LATEST_REPORT_QUERY, TABLE_NAME, TABLE_NAME, DATANODE_UUID, DATANODE_UUID, REPORT_ID, REPORT_ID,
     *                  GROUP_ID, GROUP_ID, ...
     */
    private static final String LATEST_REPORT_QUERY =
            "SELECT a.* FROM %s a LEFT OUTER JOIN %s b ON a.%s = b.%s AND a.%s = b.%s AND a.%s < b.%s WHERE b.%s IS NULL;";

    /**
     * Query to retrieve the maximum groupId for a particular DataNode UUID.
     *
     * When formatting this String, the parameters to String.format() should be as follows:
     *      String.format(MAX_GROUP_ID_QUERY, GROUP_ID, TABLE_NAME, DATANODE_UUID, datanodeUuid)
     */
    private static final String MAX_GROUP_ID_QUERY =
            "SELECT max(%s) FROM %s WHERE %s = \"%s\"";

    /**
     * This is used to retrieve all StorageReport instances with a groupId strictly greater than the one specified
     * by the caller.
     *
     * When formatting this String, the parameters to String.format() should be as follows:
     *      String.format(GREATER_THAN_GROUP_ID_QUERY, TABLE_NAME, GROUP_ID, groupId)
     */
    private static final String GREATER_THAN_GROUP_ID_QUERY =
            "SELECT * from %s WHERE %s > %d";

    /**
     * Used to delete all StorageReport instances associated with a given DataNode.
     *
     * When formatting this String, the parameters to String.format() should be as follows:
     *      String.format(DELETE_STORAGE_REPORTS_QUERY, TABLE_NAME, DATANODE_UUID, datanodeUuid)
     */
    private static final String DELETE_STORAGE_REPORTS_QUERY =
            "DELETE FROM %s WHERE %s = %s";

    @Override
    public StorageReport getStorageReport(long groupId, int reportId, String datanodeUuid) throws StorageException {
        LOG.info("GET StorageReport groupId = " + groupId + ", reportId = " + reportId);

        HopsSession session = connector.obtainSession();

        Object[] primaryKey = {groupId, reportId, datanodeUuid};
        StorageReportDTO report = session.find(StorageReportDTO.class, primaryKey);

        if (report == null)
            return null;

        return convert(report); // Convert the StorageReportDTO to a StorageReport and return it.
    }

    @Override
    public void removeStorageReport(long groupId, int reportId, String datanodeUuid) throws StorageException {
        LOG.info("REMOVE StorageReport groupId = " + groupId + ", reportId = " + reportId);

        HopsSession session = connector.obtainSession();
        Object[] primaryKey = {groupId, reportId, datanodeUuid};
        StorageReportDTO deleteMe = session.find(StorageReportDTO.class, primaryKey);
        session.deletePersistent(StorageReportDTO.class, deleteMe);

        LOG.debug("Successfully removed/deleted DatanodeStorage with groupId = " + groupId
            + ", reportId = " + reportId);
    }

    @Override
    public int removeStorageReports(long groupId, String datanodeUuid) throws StorageException {
        LOG.info("REMOVE StorageReport group " + groupId + ", datanodeUuid = " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<StorageReportDTO> domainType = queryBuilder.createQueryDefinition(StorageReportDTO.class);

        HopsPredicate predicateGroupId = domainType.get("groupId").equal(domainType.param("groupIdParam"));
        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(predicateGroupId.and(predicateDatanodeUuid));

        domainType.where(predicateGroupId.and(predicateDatanodeUuid));

        HopsQuery<StorageReportDTO> query = session.createQuery(domainType);
        query.setParameter("groupIdParam", groupId);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<StorageReportDTO> storageReportDTOs = query.getResultList();

        session.deletePersistentAll(storageReportDTOs);
        session.release(storageReportDTOs);

        // Return the number of Storage Reports that were deleted.
        return storageReportDTOs.size();
    }

    @Override
    public int removeStorageReports(String datanodeUuid) throws StorageException {
        LOG.info("DELETE StorageReports for DN " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<StorageReportDTO> domainType = queryBuilder.createQueryDefinition(StorageReportDTO.class);

        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(predicateDatanodeUuid);

        HopsQuery<StorageReportDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<StorageReportDTO> storageReportDTOs = query.getResultList();

        session.deletePersistentAll(storageReportDTOs);
        session.release(storageReportDTOs);

        // Return the number of Storage Reports that were deleted.
        return storageReportDTOs.size();
    }

    @Override
    public void addStorageReport(StorageReport storageReport) throws StorageException {
        // LOG.info("ADD StorageReport " + storageReport.toString());

        StorageReportDTO dtoObject = null;
        HopsSession session = connector.obtainSession();

        try {
            dtoObject = session.newInstance(StorageReportDTO.class);
            copyState(dtoObject, storageReport);
            session.savePersistent(dtoObject);

            /*LOG.debug("Wrote/persisted StorageReport groupId = " + dtoObject.getGroupId() + ", reportId = "
                    + dtoObject.getReportId() + " to MySQL NDB storage.");*/
        } finally {
            session.release(dtoObject);
        }
    }

    @Override
    public int getLastGroupId(String datanodeUuid) throws StorageException {
        LOG.info("GET largest groupId for DN: " + datanodeUuid);

        return getMaxGroupId(datanodeUuid);
    }

    @Override
    public List<StorageReport> getStorageReportsAfterGroupId(long groupId, String datanodeUuid) throws StorageException {
        LOG.info("GET StorageReports after group " + groupId + ", DN UUID: " + datanodeUuid);

        PreparedStatement s = null;
        ResultSet result = null;

        String query = String.format(GREATER_THAN_GROUP_ID_QUERY, TABLE_NAME, GROUP_ID, groupId);
        LOG.debug("Executing MySQL query: " + query);

        ArrayList<StorageReport> resultList = new ArrayList<>();

        try {
            Connection conn = mysqlConnector.obtainSession();
            s = conn.prepareStatement(query);
            result = s.executeQuery();

            //LOG.debug("Result = " + result.toString());

            while (result.next()) {
                StorageReport report = new StorageReport(
                    result.getLong(GROUP_ID), result.getInt(REPORT_ID), result.getString(DATANODE_UUID),
                        result.getBoolean(FAILED), result.getLong(CAPACITY), result.getLong(DFS_USED),
                        result.getLong(REMAINING), result.getLong(BLOCK_POOL_USED),
                        result.getString(DATANODE_STORAGE_ID)
                );

                //LOG.debug("Retrieved StorageReport instance: " + report.toString());

                resultList.add(report);
            }
        } catch (SQLException ex) {
            throw HopsSQLExceptionHelper.wrap(ex);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException ex) {
                    LOG.warn("Exception when closing the PrepareStatement", ex);
                }
            }

            if (result != null) {
                try {
                    result.close();
                } catch (SQLException ex) {
                    LOG.warn("Exception when closing the ResultSet", ex);
                }
            }

            mysqlConnector.closeSession();
        }

        return resultList;
    }

    @Override
    public List<StorageReport> getStorageReports(long groupId, String datanodeUuid) throws StorageException {
        LOG.info("GET StorageReport group " + groupId + ", DN UUID: " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<StorageReportDTO> domainType = queryBuilder.createQueryDefinition(StorageReportDTO.class);

        HopsPredicate predicateGroupId = domainType.get("groupId").equal(domainType.param("groupIdParam"));
        HopsPredicate predicateDatanodeUuid =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(predicateGroupId.and(predicateDatanodeUuid));

        HopsQuery<StorageReportDTO> query = session.createQuery(domainType);
        query.setParameter("groupIdParam", groupId);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<StorageReportDTO> storeReportDTOs = query.getResultList();

        ArrayList<StorageReport> resultList = new ArrayList<>();

        for (StorageReportDTO dto : storeReportDTOs) {
            resultList.add(convert(dto));
        }

        return resultList;
    }

    private int getMaxGroupId(String datanodeUuid) throws StorageException {
        String query = String.format(MAX_GROUP_ID_QUERY, GROUP_ID, TABLE_NAME, DATANODE_UUID, datanodeUuid);

        LOG.debug("Executing MySQL query: " + query);

        PreparedStatement s = null;
        ResultSet result = null;

        try {
            Connection conn = mysqlConnector.obtainSession();
            s = conn.prepareStatement(query);
            result = s.executeQuery();

            LOG.debug("Result = " + result.toString());

            if (result.next())
                return result.getInt(String.format("max(%s)", GROUP_ID));
            else
                throw new StorageException(
                        "No groupId returned when attempting to find max groupId for datanode " + datanodeUuid);

        } catch (SQLException ex) {
            throw HopsSQLExceptionHelper.wrap(ex);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException ex) {
                    LOG.warn("Exception when closing the PrepareStatement", ex);
                }
            }

            if (result != null) {
                try {
                    result.close();
                } catch (SQLException ex) {
                    LOG.warn("Exception when closing the ResultSet", ex);
                }
            }

            mysqlConnector.closeSession();
        }
    }

    @Override
    public List<StorageReport> getLatestStorageReports(String datanodeUuid) throws StorageException {
        LOG.info("GET Latest StorageReports from DN " + datanodeUuid);

        int maxGroupId = getMaxGroupId(datanodeUuid);

        LOG.debug("Max groupId: " + maxGroupId);

        return getStorageReports(maxGroupId, datanodeUuid);
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
        dest.setDatanodeUuid(src.getDatanodeUuid());
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

        return new StorageReport(src.getGroupId(), src.getReportId(), src.getDatanodeUuid(), failed,
                src.getCapacity(), src.getDfsUsed(), src.getRemaining(), src.getBlockPoolUsed(),
                src.getDatanodeStorageId());
    }
}
