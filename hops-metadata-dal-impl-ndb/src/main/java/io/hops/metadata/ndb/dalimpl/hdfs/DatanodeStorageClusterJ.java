package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.DatanodeStorageDataAccess;
import io.hops.metadata.hdfs.entity.DatanodeStorage;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class DatanodeStorageClusterJ
        implements TablesDef.DatanodeStoragesTableDef, DatanodeStorageDataAccess<DatanodeStorage> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface DatanodeStorageDTO {
        @PrimaryKey
        @Column(name = STORAGE_ID)
        String getStorageId();
        void setStorageId(String storageId);

        @Column(name = STATE)
        int getState();
        void setState(int state);

        @Column(name = STORAGE_TYPE)
        int getStorageType();
        void setStorageType(int storageType);
    }

    private static final Log LOG = LogFactory.getLog(DatanodeStorageClusterJ.class);

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public DatanodeStorage getDatanodeStorage(String storageId) throws StorageException {
        LOG.info("GET DatanodeStorage " + storageId);

        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<DatanodeStorageDTO> domainType =
                queryBuilder.createQueryDefinition(DatanodeStorageDTO.class);
        domainType.where(domainType.get("name").equal(domainType.param("param")));
        HopsQuery<DatanodeStorageDTO> query = session.createQuery(domainType);
        query.setParameter("param", storageId);

        List<DatanodeStorageDTO> results = query.getResultList();
        DatanodeStorage datanodeStorage = null;

        if (results.size() == 1) {
            DatanodeStorageDTO result = results.get(0);

            datanodeStorage = new DatanodeStorage(result.getStorageId(), result.getState(), result.getStorageType());
        }

        session.release(results);

        return datanodeStorage;
    }

    @Override
    public void removeDatanodeStorage(String storageId) throws StorageException {

    }

    @Override
    public void addDatanodeStorage(DatanodeStorage datanodeStorage) throws StorageException {

    }
}
