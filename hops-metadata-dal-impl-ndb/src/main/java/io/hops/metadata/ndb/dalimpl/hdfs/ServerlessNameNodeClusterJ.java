package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.ServerlessNameNodeDataAccess;
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

        @PrimaryKey
        @Column(name = FUNCTION_NAME)
        String getFunctionName();
        void setFunctionName(String functionName);

        @Column(name = REPLICA_ID)
        String getReplicaId();
        void setReplicaId(String replicaId);

        @Column(name = CREATION_TIME)
        long getCreationTime();
        void setCreationTime(long creationTime);
    }

    @Override
    public ServerlessNameNodeMeta getServerlessNameNode(long nameNodeId, String functionName) throws StorageException {
        LOG.debug("GET NameNode (ID = " + nameNodeId + ", function name = " + functionName + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        HopsPredicate idPredicate = domainType.get("namenode_id").equal(domainType.param("namenode_id_param"));
        HopsPredicate functionNamePredicate =
                domainType.get("function_name").equal(domainType.param("function_name_param"));
        domainType.where(idPredicate.and(functionNamePredicate));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("namenode_id_param", nameNodeId);
        query.setParameter("function_name_param", functionName);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        ServerlessNameNodeMeta result = null;
        if (results.size() == 1) {
            ServerlessNameNodeDTO resultDTO = results.get(0);
            result = convert(resultDTO);
        } else if (results.size() > 1) {
            LOG.warn("Unexpected results length: " + results.size() + ".");
            LOG.warn("Returning result with the latest creation time...");

            long largestCreationTime = -1;
            ServerlessNameNodeDTO latestDTO = null;

            for (ServerlessNameNodeDTO resultDTO : results) {
                if (resultDTO.getCreationTime() > largestCreationTime) {
                    largestCreationTime = resultDTO.getCreationTime();
                    latestDTO = resultDTO;
                }
            }

            if (latestDTO != null)
                result = convert(latestDTO);
        } else {
            LOG.warn("Failed to find a NameNode with ID = " + nameNodeId + " and serverless function name = "
                + functionName + ".");
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

            long largestCreationTime = -1;
            ServerlessNameNodeDTO latestDTO = null;

            for (ServerlessNameNodeDTO resultDTO : results) {
                if (resultDTO.getCreationTime() > largestCreationTime) {
                    largestCreationTime = resultDTO.getCreationTime();
                    latestDTO = resultDTO;
                }
            }

            if (latestDTO != null)
                result = convert(latestDTO);
        } else {
            LOG.warn("Failed to find a NameNode with ID = " + nameNodeId + ".");
        }

        session.release(results);
        return result;
    }

    @Override
    public ServerlessNameNodeMeta getServerlessNameNodeByFunctionName(String functionName) throws StorageException {
        LOG.debug("GET NameNode (function name = " + functionName + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        domainType.where(domainType.get("function_name").equal(domainType.param("function_name_param")));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("function_name_param", functionName);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        ServerlessNameNodeMeta result = null;
        if (results.size() == 1) {
            ServerlessNameNodeDTO resultDTO = results.get(0);
            result = convert(resultDTO);
        } else if (results.size() > 1) {
            LOG.warn("Unexpected results length: " + results.size() + ".");
            LOG.warn("Returning result with the latest creation time...");

            long largestCreationTime = -1;
            ServerlessNameNodeDTO latestDTO = null;

            for (ServerlessNameNodeDTO resultDTO : results) {
                if (resultDTO.getCreationTime() > largestCreationTime) {
                    largestCreationTime = resultDTO.getCreationTime();
                    latestDTO = resultDTO;
                }
            }

            if (latestDTO != null)
                result = convert(latestDTO);
        } else {
            LOG.warn("Failed to find a NameNode with function name = " + functionName + ".");
        }

        session.release(results);
        return result;
    }

    @Override
    public void addServerlessNameNode(ServerlessNameNodeMeta nameNode) throws StorageException {
        LOG.debug("ADD Serverless NameNode " + nameNode.toString());
        ServerlessNameNodeDTO serverlessNameNodeDTO = null;
        HopsSession session = connector.obtainSession();

        try {
            serverlessNameNodeDTO = session.newInstance(ServerlessNameNodeDTO.class);
            copyState(serverlessNameNodeDTO, nameNode);
            session.savePersistent(serverlessNameNodeDTO);
            LOG.debug("Wrote/persisted ServerlessNameNode (ID = " + nameNode.getNameNodeId() + ", function name = "
                    + nameNode.getFunctionName() + ") to MySQL NDB storage.");
        } finally {
            session.release(serverlessNameNodeDTO);
        }
    }

    @Override
    public void replaceServerlessNameNode(ServerlessNameNodeMeta nameNode) throws StorageException {
        LOG.debug("REPLACE Serverless NameNode, new instance = " + nameNode.toString());

        removeServerlessNameNode(nameNode.getFunctionName());
        addServerlessNameNode(nameNode);
    }

    @Override
    public void removeServerlessNameNode(ServerlessNameNodeMeta nameNode) throws StorageException {
        LOG.debug("REMOVE NameNode (ID = " + nameNode.getNameNodeId() + ")");

        HopsSession session = connector.obtainSession();

        final Object[] key = new Object[2];
        key[0] = nameNode.getNameNodeId();
        key[1] = nameNode.getFunctionName();

        ServerlessNameNodeDTO deleteMe
                = session.find(ServerlessNameNodeDTO.class, key);

        session.deletePersistent(deleteMe);
    }

    @Override
    public void removeServerlessNameNode(String functionName) throws StorageException {
        LOG.debug("REMOVE NameNode (function name = " + functionName + ")");

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<ServerlessNameNodeDTO> domainType
                = queryBuilder.createQueryDefinition(ServerlessNameNodeDTO.class);

        domainType.where(domainType.get(FUNCTION_NAME).equal(domainType.param("function_name_param")));

        HopsQuery<ServerlessNameNodeDTO> query = session.createQuery(domainType);
        query.setParameter("function_name_param", functionName);

        List<ServerlessNameNodeDTO> results = query.getResultList();

        if (results.size() == 0) {
            LOG.error("There are no Serverless NameNode instances with function name = "
                    + functionName + " in NDB.");
            return;
        }
        else if (results.size() > 1) {
            LOG.warn("There are multiple Serverless NN instances with function name = " +
                    functionName + " in NDB. Deleting ALL of them...");
        }

        for (ServerlessNameNodeDTO deleteMe : results) {
            LOG.debug("Deleting Serverless NN instance: " + deleteMe.toString());
            session.deletePersistent(deleteMe);
        }

        session.release(results);
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

        // Convert each ServerlessNameNodeDTO object to a ServerlessNameNodeMeta object and add it to the list.
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
        return new ServerlessNameNodeMeta(src.getNameNodeId(), src.getFunctionName(),
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
        dest.setFunctionName(src.getFunctionName());
        dest.setReplicaId(src.getReplicaId());
        dest.setCreationTime(src.getCreationTime());
    }
}
