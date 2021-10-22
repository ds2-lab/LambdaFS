package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.ServerlessNameNodeDataAccess;
import io.hops.metadata.hdfs.entity.DataNodeMeta;
import io.hops.metadata.hdfs.entity.ServerlessNameNodeMeta;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines ClusterJ interface for reading and writing serverless name node metadata instances
 * to/from MySQL Cluster NDB.
 */
public class ServerlessNameNodeClusterJ implements TablesDef.ServerlessNameNodesTableDef,
        ServerlessNameNodeDataAccess<ServerlessNameNodeMeta> {
    private static final Log LOG = LogFactory.getLog(ServerlessNameNodeClusterJ.class);
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @PersistenceCapable(table = TABLE_NAME)
    public interface ServerlessNameNodeDTO {
        @PrimaryKey
        @Column(name = NAME_NODE_ID)
        long getNameNodeId();
        void setNameNodeId(long nameNodeId);

        @Column(name = DEPLOYMENT_ID)
        int getDeploymentId();
        void setDeploymentId(int deploymentId);

        @Column(name = REPLICA_ID)
        String getReplicaId();
        void setReplicaId(String replicaId);

        @Column(name = CREATION_TIME)
        long getCreationTime();
        void setCreationTime(long creationTime);
    }

//    private HopsQuery<ServerlessNameNodeDTO> createQuery(List<String> columns, HopsSession session)
//            throws StorageException {
//        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
//        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
//                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);
//
//
//        for (String column : columns) {
//            String param = column + "_param";
//        }
//
//        HopsPredicate idPredicate = domainType.get("namenode_id").equal(domainType.param("namenode_id_param"));
//        HopsPredicate deploymentNumberPredicate =
//                domainType.get("deployment_id").equal(domainType.param("deployment_id_param"));
//        domainType.where(idPredicate.and(deploymentNumberPredicate));
//
//        return session.createQuery(domainType);
//    }

    @Override
    public ServerlessNameNodeMeta getServerlessNameNode(long nameNodeId, int deploymentNumber) throws StorageException {
        LOG.debug("GET NameNode (ID = " + nameNodeId + ", deployment number = " + deploymentNumber + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        HopsPredicate idPredicate = domainType.get("namenode_id").equal(domainType.param("namenode_id_param"));
        HopsPredicate deploymentNumberPredicate =
                domainType.get("deployment_id").equal(domainType.param("deployment_id_param"));
        domainType.where(idPredicate.and(deploymentNumberPredicate));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("namenode_id_param", nameNodeId);
        query.setParameter("deployment_id_param", deploymentNumber);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        ServerlessNameNodeMeta result = null;
        if (results.size() == 1) {
            ServerlessNameNodeDTO resultDTO = results.get(0);
            result = convert(resultDTO);
        } else if (results.size() > 1) {
            LOG.warn("Unexpected results length: " + results.size() + ".");
            LOG.warn("Returning result with the latest creation time...");
        } else {
            LOG.warn("Failed to find a NameNode with ID = " + nameNodeId + " and deployment number = "
                + deploymentNumber + ".");
        }

        session.release(results);
        return result;
    }

    @Override
    public ServerlessNameNodeMeta getServerlessNameNodeByNameNodeId(long nameNodeId) throws StorageException {
        LOG.debug("GET NameNode (ID = " + nameNodeId + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        domainType.where(domainType.get("namenode_id").equal(domainType.param("namenode_id_param")));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("namenode_id_param", nameNodeId);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        ServerlessNameNodeMeta result = null;
        if (results.size() == 1) {
            ServerlessNameNodeDTO resultDTO = results.get(0);
            result = convert(resultDTO);
        } else if (results.size() > 1) {
            LOG.warn("Unexpected results length: " + results.size() + ".");
            LOG.warn("Returning result with the latest creation time...");
        } else {
            LOG.warn("Failed to find a NameNode with ID = " + nameNodeId + ".");
        }

        session.release(results);
        return result;
    }

    @Override
    public ServerlessNameNodeMeta getServerlessNameNodeByDeploymentNumber(long deploymentNumber) throws StorageException {
        LOG.debug("GET NameNode (deployment number = " + deploymentNumber + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        domainType.where(domainType.get("deployment_id").equal(domainType.param("deployment_id_param")));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("deployment_id_param", deploymentNumber);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        ServerlessNameNodeMeta result = null;
        if (results.size() == 1) {
            ServerlessNameNodeDTO resultDTO = results.get(0);
            result = convert(resultDTO);
        } else if (results.size() > 1) {
            LOG.warn("Unexpected results length: " + results.size() + ".");
            LOG.warn("Returning result with the latest creation time...");
        } else {
            LOG.warn("Failed to find a NameNode with deployment number = " + deploymentNumber + ".");
        }

        session.release(results);
        return result;
    }

    @Override
    public void addServerlessNameNode(ServerlessNameNodeMeta nameNode) throws StorageException {
        LOG.debug("ADD DataNode " + nameNode.toString());
        ServerlessNameNodeDTO dataNodeDTO = null;
        //LOG.info("Obtaining HopsSession now...");
        HopsSession session = connector.obtainSession();
        //LOG.info("Successfully obtained HopsSession.");

        try {
            dataNodeDTO = session.newInstance(ServerlessNameNodeDTO.class);
            //LOG.info("Created new instance of DataNodeDTO...");
            copyState(dataNodeDTO, nameNode);
            //LOG.info("Successfully copied DataNode state to DataNodeDTO.");
            session.savePersistent(dataNodeDTO);
            LOG.debug("Wrote/persisted DataNode " + nameNode.getNameNodeId() + "(deployment number = "
                    + nameNode.getDeploymentNumber() + ") to MySQL NDB storage.");
        } finally {
            session.release(dataNodeDTO);
            //LOG.info("Released DataNodeDTO instance.");
        }
    }

    @Override
    public List<ServerlessNameNodeMeta> getAllServerlessNameNodes() throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();

        HopsQueryDomainType<ServerlessNameNodeDTO> queryDomainType =
                queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);
        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(queryDomainType);
        List<ServerlessNameNodeDTO> resultsRaw = query.getResultList();

        List<ServerlessNameNodeMeta> results = new ArrayList<>();

        // Convert each ServerlessNameNodeDTO object to a DataNodeMeta object and add it to the list.
        for (ServerlessNameNodeDTO serverlessNameNodeDTO : resultsRaw) {
            results.add(convert(serverlessNameNodeDTO));
        }

        return results;
    }

    /**
     * Convert the given {@link ServerlessNameNodeDTO} object to a {@link ServerlessNameNodeMeta} object.
     * @param src The {@link ServerlessNameNodeDTO} object from which the {@link ServerlessNameNodeMeta}
     *            will be created.
     * @return Instance of {@link ServerlessNameNodeMeta} created with the fields of the given
     * {@link ServerlessNameNodeDTO} object.
     */
    private ServerlessNameNodeMeta convert(ServerlessNameNodeDTO src) {
        return new ServerlessNameNodeMeta(src.getNameNodeId(), src.getDeploymentId(),
                src.getReplicaId(), src.getCreationTime());
    }

    /**
     * Copy the state from the given {@link ServerlessNameNodeMeta} instance to the given
     * {@link ServerlessNameNodeDTO} instance.
     * @param dest The ServerlessNameNodeDTO destination object.
     * @param src The ServerlessNameNodeMeta source object.
     */
    private void copyState(ServerlessNameNodeDTO dest, ServerlessNameNodeMeta src) {
        dest.setNameNodeId(src.getNameNodeId());
        dest.setDeploymentId(src.getDeploymentNumber());
        dest.setReplicaId(src.getReplicaId());
        dest.setCreationTime(src.getCreationTime());
    }
}
