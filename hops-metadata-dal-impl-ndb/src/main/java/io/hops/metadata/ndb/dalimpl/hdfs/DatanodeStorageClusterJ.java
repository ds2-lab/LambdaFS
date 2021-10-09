package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.DatanodeStorageDataAccess;
import io.hops.metadata.hdfs.entity.DataNodeMeta;
import io.hops.metadata.hdfs.entity.DatanodeStorage;
import io.hops.metadata.hdfs.entity.StorageReport;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class DatanodeStorageClusterJ
        implements TablesDef.DatanodeStoragesTableDef, DatanodeStorageDataAccess<DatanodeStorage> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface DatanodeStorageDTO {
        @PrimaryKey
        @Column(name = STORAGE_ID)
        String getStorageId();
        void setStorageId(String storageId);

        @PrimaryKey
        @Column(name = DATANODE_UUID)
        String getDatanodeUuid();
        void setDatanodeUuid(String datanodeUuid);

        @Column(name = STATE)
        int getState();
        void setState(int state);

        @Column(name = STORAGE_TYPE)
        int getStorageType();
        void setStorageType(int storageType);
    }

    private static final Log LOG = LogFactory.getLog(DatanodeStorageClusterJ.class);

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    /**
     * Retrieve a particular DatanodeStorage from NDB identified by the provided storageId.
     * @param storageId Identifies the DatanodeStorage.
     */
    @Override
    public DatanodeStorage getDatanodeStorage(String storageId, String datanodeUuid) throws StorageException {
        LOG.info("GET DatanodeStorage with ID: " + storageId + ", DN UUID: " + datanodeUuid);

        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<DatanodeStorageDTO> domainType =
                queryBuilder.createQueryDefinition(DatanodeStorageDTO.class);
        HopsPredicate storageIdPredicate = domainType.get("storageId").equal(domainType.param("storageIdParam"));
        HopsPredicate datanodeUuidPredicate =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));


        domainType.where(storageIdPredicate.and(datanodeUuidPredicate));
        HopsQuery<DatanodeStorageDTO> query = session.createQuery(domainType);
        query.setParameter("storageIdParam", storageId);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<DatanodeStorageDTO> results = query.getResultList();
        DatanodeStorage datanodeStorage = null;

        if (results.size() == 1) {
            DatanodeStorageDTO result = results.get(0);

            datanodeStorage = new DatanodeStorage(result.getStorageId(), result.getDatanodeUuid(),
                    result.getState(), result.getStorageType());
        }

        session.release(results);

        return datanodeStorage;
    }

    @Override
    public List<DatanodeStorage> getDatanodeStorages(String datanodeUuid) throws StorageException {
        LOG.info("GET DatanodeStorages associated with DataNode " + datanodeUuid);

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<DatanodeStorageDTO> domainType =
                queryBuilder.createQueryDefinition(DatanodeStorageDTO.class);
        HopsPredicate datanodeUuidPredicate =
                domainType.get("datanodeUuid").equal(domainType.param("datanodeUuidParam"));
        domainType.where(datanodeUuidPredicate);

        HopsQuery<DatanodeStorageDTO> query = session.createQuery(domainType);
        query.setParameter("datanodeUuidParam", datanodeUuid);

        List<DatanodeStorageDTO> dtoResults = query.getResultList();

        LOG.debug("Query result contained " + dtoResults.size() + " entries.");

        List<DatanodeStorage> results = new ArrayList<>();

        for (DatanodeStorageDTO result : dtoResults) {
            // Convert each DatanodeStorageDTO object to a DatanodeStorage object, then add it to the list.
            results.add(convert(result));
        }

        return results;
    }

    /**
     * Remove a particular DatanodeStorage from NDB identified by the given storageId.
     * @param storageId The ID of the DatanodeStorage instance to remove from NDB.
     */
    @Override
    public void removeDatanodeStorage(String storageId, String datanodeUuid) throws StorageException {
        LOG.info("REMOVE DatanodeStorage " + storageId);

        HopsSession session = connector.obtainSession();
        Object[] primaryKey = {storageId, datanodeUuid};
        DatanodeStorageDTO deleteMe = session.find(DatanodeStorageDTO.class, primaryKey);
        session.deletePersistent(DatanodeStorageDTO.class, deleteMe);

        LOG.debug("Successfully removed/deleted DatanodeStorage with storageId "
                + storageId + ", datanodeUuid = " + datanodeUuid);
    }

    @Override
    public int removeDatanodeStorages(String datanodeUuid) throws StorageException {
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder qb = session.getQueryBuilder();
        HopsQueryDomainType<DatanodeStorageDTO> dobj = qb.createQueryDefinition(DatanodeStorageDTO.class);
        HopsPredicate pred1 = dobj.get("datanodeUuid").equal(dobj.param("datanodeUuidParam"));
        dobj.where(pred1);

        HopsQuery<DatanodeStorageDTO> query = session.createQuery(dobj);
        query.setParameter("datanodeUuidParam", datanodeUuid);
        List<DatanodeStorageDTO> dtos = query.getResultList();

        session.deletePersistentAll(dtos);
        session.release(dtos);

        // Return the number of Datanode Storage instances that were deleted.
        return dtos.size();
    }

    @Override
    public void addDatanodeStorage(DatanodeStorage datanodeStorage) throws StorageException {
        // LOG.info("ADD DatanodeStorage " + datanodeStorage.toString());
        DatanodeStorageDTO toAdd = null;
        HopsSession session = connector.obtainSession();

        try {
            toAdd = session.newInstance(DatanodeStorageDTO.class);
            copyState(toAdd, datanodeStorage);
            session.savePersistent(toAdd);

            // LOG.debug("Wrote/persisted DatanodeStorage " + toAdd.getStorageId() + " to MySQL NDB storage.");
        } finally {
            session.release(toAdd);
        }
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.DatanodeStorage} instance to the given
     * {@link io.hops.metadata.ndb.dalimpl.hdfs.DatanodeStorageClusterJ.DatanodeStorageDTO} instance.
     * @param dest The DatanodeStorageDTO destination object.
     * @param src The DatanodeStorage source object.
     */
    private void copyState(DatanodeStorageDTO dest, DatanodeStorage src) {
        dest.setStorageId(src.getStorageId());
        dest.setDatanodeUuid(src.getDatanodeUuid());
        dest.setState(src.getState());
        dest.setStorageType(src.getStorageType());
    }

    /**
     * Convert the given DatanodeStorageDTO instance to a DatanodeStorage instance.
     */
    private static DatanodeStorage convert(DatanodeStorageDTO src) {
        return new DatanodeStorage(
                src.getStorageId(), src.getDatanodeUuid(), src.getState(), src.getStorageType());
    }
}
