package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.DataNodeDataAccess;
import io.hops.metadata.hdfs.entity.DataNodeMeta;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class DataNodeClusterJ implements TablesDef.DataNodesTableDef, DataNodeDataAccess<DataNodeMeta> {
    private static final Log LOG = LogFactory.getLog(DataNodeClusterJ.class);

    @PersistenceCapable(table = TABLE_NAME)
    public interface DataNodeDTO {
        @PrimaryKey
        @Column(name = DATANODE_UUID)
        String getDatanodeUuid();
        void setDatanodeUuid(String datanodeUuid);

        @Column(name = HOSTNAME)
        String getHostname();
        void setHostname(String hostname);

        @Column(name = IP_ADDR)
        String getIpAddress();
        void setIpAddress(String ipAddr);

        @Column(name = XFER_PORT)
        int getXferPort();
        void setXferPort(int xferPort);

        @Column(name = INFO_PORT)
        int getInfoPort();
        void setInfoPort(int infoPort);

        @Column(name = INFO_SECURE_PORT)
        int getInfoSecurePort();
        void setInfoSecurePort(int infoSecurePort);

        @Column(name = IPC_PORT)
        int getIpcPort();
        void setIpcPort(int ipcPort);

        @Column(name = CREATION_TIME)
        long getCreationTime();
        void setCreationTime(long creationTime);
    }

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    /**
     * Retrieve a particular DataNode from the intermediate storage identified by the given UUID.
     * @param uuid The UUID of the DataNode to retrieve.
     */
    @Override
    public DataNodeMeta getDataNode(final String uuid) throws StorageException {
        LOG.info("GET DataNode " + uuid);
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<DataNodeDTO> domainType = queryBuilder.createQueryDefinition(DataNodeDTO.class);
        domainType.where(domainType.get("name").equal(domainType.param("param"))); // Not sure why we need this line?
        HopsQuery<DataNodeDTO> query = session.createQuery(domainType);
        query.setParameter("param", uuid);
        List<DataNodeDTO> results = query.getResultList();

        DataNodeMeta dataNode = null;

        if (results.size() == 1) {
            DataNodeDTO dataNodeDTO = results.get(0);
            dataNode = convert(dataNodeDTO);
        }

        session.release(results);
        return dataNode;
    }

    /**
     * Remove a given DataNode from the intermediate storage.
     * @param uuid The UUID of the DataNode to remove.
     *
     * TODO: Fix this method. Doesn't seem to work. Maybe need to pass a DataNodeDTO object.
     */
    @Override
    public void removeDataNode(String uuid) throws StorageException {
        LOG.debug("REMOVE DataNode " + uuid);
        HopsSession session = connector.obtainSession();

        DataNodeDTO deleteMe = session.find(DataNodeDTO.class, uuid);

        session.deletePersistent(deleteMe);

        LOG.debug("Successfully deleted metadata for DataNode " + uuid);
    }

    /**
     * Add a DatNode to the intermediate storage.
     * @param dataNode The DataNode to add to intermediate storage.
     */
    @Override
    public void addDataNode(DataNodeMeta dataNode) throws StorageException {
        LOG.debug("ADD DataNode " + dataNode.toString());
        DataNodeDTO dataNodeDTO = null;
        //LOG.info("Obtaining HopsSession now...");
        HopsSession session = connector.obtainSession();
        //LOG.info("Successfully obtained HopsSession.");

        try {
            dataNodeDTO = session.newInstance(DataNodeDTO.class);
            //LOG.info("Created new instance of DataNodeDTO...");
            copyState(dataNodeDTO, dataNode);
            //LOG.info("Successfully copied DataNode state to DataNodeDTO.");
            session.savePersistent(dataNodeDTO);
            LOG.debug("Wrote/persisted DataNode " + dataNode.getDatanodeUuid() + " to MySQL NDB storage.");
        } finally {
            session.release(dataNodeDTO);
            //LOG.info("Released DataNodeDTO instance.");
        }
    }

    /**
     * Retrieve all of the DataNodes stored in intermediate storage.
     * @return A list containing all of the DataNodes stored in the intermediate storage.
     */
    @Override
    public List<DataNodeMeta> getAllDataNodes() throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();

        HopsQueryDomainType<DataNodeDTO> queryDomainType = queryBuilder.createQueryDefinition(DataNodeDTO.class);
        HopsQuery<DataNodeDTO> query = session.createQuery(queryDomainType);
        List<DataNodeDTO> resultsRaw = query.getResultList();

        List<DataNodeMeta> results = new ArrayList<>();

        // Convert each DataNodeDTO object to a DataNodeMeta object and add it to the list.
        for (DataNodeDTO dataNodeDTO : resultsRaw) {
            results.add(convert(dataNodeDTO));
        }

        return results;
    }

    /**
     * Convert the given {@link io.hops.metadata.ndb.dalimpl.hdfs.DataNodeClusterJ.DataNodeDTO} instance
     * to an object of type {@link io.hops.metadata.hdfs.entity.DataNodeMeta}.
     * @param src The DataNodeDTO source object.
     * @return An instance of DataNodeMeta whose instance variables have been populated from the {@code src} parameter.
     */
    private DataNodeMeta convert(DataNodeDTO src) {
        return new DataNodeMeta(
                src.getDatanodeUuid(), src.getHostname(), src.getIpAddress(),
                src.getXferPort(), src.getInfoPort(), src.getInfoSecurePort(),
                src.getIpcPort(), src.getCreationTime()
        );
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.DataNodeMeta} instance to the given
     * {@link io.hops.metadata.ndb.dalimpl.hdfs.DataNodeClusterJ.DataNodeDTO} instance.
     * @param dest The DataNodeDTO destination object.
     * @param src The DataNodeMeta source object.
     */
    private void copyState(DataNodeDTO dest, DataNodeMeta src) {
        dest.setDatanodeUuid(src.getDatanodeUuid());
        dest.setHostname(src.getHostname());
        dest.setIpAddress(src.getIpAddress());
        dest.setXferPort(src.getXferPort());
        dest.setInfoPort(src.getInfoPort());
        dest.setInfoSecurePort(src.getInfoSecurePort());
        dest.setIpcPort(src.getIpcPort());
        dest.setCreationTime(src.getCreationTime());
    }
}
