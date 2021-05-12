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

import java.util.List;

public class DataNodeClusterJ implements TablesDef.DataNodesTableDef, DataNodeDataAccess<DataNodeMeta> {
    private static final Log LOG = LogFactory.getLog(EncodingStatusClusterj.class);

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
        void setIpAddr(String ipAddr);

        @Column(name = XFER_PORT)
        int getXferPort();
        void setXferPort(int xferPort);

        @Column(name = INFO_PORT)
        int getInfoPort();
        void setInfoPort(int infoPort);

        @Column(name = IPC_PORT)
        int getIpcPort();
        void setIpcPort(int ipcPort);
    }

    private ClusterjConnector connector = ClusterjConnector.getInstance();

    /**
     * Retrieve a given DataNode from the intermediate storage.
     * @param uuid The UUID of the DataNode to retrieve.
     */
    @Override
    public DataNodeMeta getDataNode(final String uuid) throws StorageException {
        LOG.info("GET DataNode " + uuid);
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<DataNodeDTO> domainType = queryBuilder.createQueryDefinition(DataNodeDTO.class);
        domainType.where(domainType.get("name").equal(domainType.param("param")));
        HopsQuery<DataNodeDTO> query = session.createQuery(domainType);
        query.setParameter("param", uuid);
        List<DataNodeDTO> results = query.getResultList();

        DataNodeMeta dataNode = null;

        if (results.size() == 1) {
            DataNodeDTO dataNodeDTO = results.get(0);
            dataNode = new DataNodeMeta(dataNodeDTO.getDatanodeUuid(), dataNodeDTO.getHostname(), dataNodeDTO.getIpAddress(),
                    dataNodeDTO.getXferPort(), dataNodeDTO.getInfoPort(), dataNodeDTO.getIpcPort());
        }

        session.release(results);
        return dataNode;
    }

    /**
     * Remove a given DataNode from the intermediate storage.
     * @param uuid The UUID of the DataNode to remove.
     */
    @Override
    public void removeDataNode(String uuid) throws StorageException {
        LOG.info("REMOVE DataNode " + uuid);
        HopsSession session = connector.obtainSession();

        session.deletePersistent(DataNodeDTO.class, uuid);
    }

    /**
     * Add a DatNode to the intermediate storage.
     * @param dataNode The DataNode to add to intermediate storage.
     */
    @Override
    public void addDataNode(DataNodeMeta dataNode) throws StorageException {
        LOG.info("ADD DataNode " + dataNode.toString());
        DataNodeDTO dataNodeDTO = null;
        HopsSession session = connector.obtainSession();

        try {
            dataNodeDTO = session.newInstance(DataNodeDTO.class);
            copyState(dataNodeDTO, dataNode);
            session.savePersistent(dataNodeDTO);
        } finally {
            session.release(dataNodeDTO);
        }
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.DataNodeMeta} instance to the given
     * {@link io.hops.metadata.ndb.dalimpl.hdfs.DataNodeClusterJ.DataNodeDTO} instance.
     * @param dataNodeDTO The destination object.
     * @param dataNode The source object.
     */
    private void copyState(DataNodeDTO dataNodeDTO, DataNodeMeta dataNode) {
        dataNodeDTO.setDatanodeUuid(dataNode.getDatanodeUuid());
        dataNodeDTO.setHostname(dataNode.getHostname());
        dataNodeDTO.setIpAddr(dataNode.getIpAddress());
        dataNodeDTO.setXferPort(dataNode.getXferPort());
        dataNodeDTO.setInfoPort(dataNode.getInfoPort());
        dataNodeDTO.setIpcPort(dataNode.getIpcPort());
    }
}
