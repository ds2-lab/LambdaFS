package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteAcknowledgementClusterJ
        implements WriteAcknowledgementDataAccess<WriteAcknowledgement>, TablesDef.WriteAcknowledgementsTableDef {
    private static final Log LOG = LogFactory.getLog(WriteAcknowledgementClusterJ.class);
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @PersistenceCapable(table = TABLE_NAME)
    public interface WriteAcknowledgementDTO {
        @PrimaryKey
        @Column(name = NAME_NODE_ID)
        public long getNameNodeId();
        public void setNameNodeId(long nameNodeId);

        @PrimaryKey
        @Column(name = DEPLOYMENT_NUMBER)
        public int getDeploymentNumber();
        public void setDeploymentNumber(int deploymentNumber);

        @Column(name = ACKNOWLEDGED)
        public byte getAcknowledged();
        public void setAcknowledged(byte acknowledged);

        @Column(name = OPERATION_ID)
        public String getOperationId();
        public void setOperationId(String operationId);
    }

    @Override
    public WriteAcknowledgement getWriteAcknowledgement(long nameNodeId, String operationId) throws StorageException {
        LOG.debug("GET WriteAcknowledgement (nameNodeId=" + nameNodeId + ", operationId=" + operationId + ")");
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(WriteAcknowledgementDTO.class);

        // We want to match against nameNodeId and operationId.
        HopsPredicate nameNodeIdPredicate =
                domainType.get("nameNodeId").equal(domainType.param("nameNodeIdParam"));
        HopsPredicate operationIdPredicate =
                domainType.get("operationId").equal(domainType.param("operationIdParam"));

        // We need to match against BOTH, not just one.
        domainType.where(nameNodeIdPredicate.and(operationIdPredicate));

        // Create the query.
        HopsQuery<WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("nameNodeIdParam", nameNodeId);
        query.setParameter("operationIdParam", operationId);

        List<WriteAcknowledgementDTO> results = query.getResultList();
        WriteAcknowledgement writeAcknowledgement = null;
        if (results.size() == 1) {
            WriteAcknowledgementDTO writeAckDTO = results.get(0);
            writeAcknowledgement = convert(writeAckDTO);
        }

        session.release(results);
        return writeAcknowledgement;
    }

    @Override
    public void acknowledge(WriteAcknowledgement writeAcknowledgement) throws StorageException {
        LOG.debug("ACK " + writeAcknowledgement.toString());

        if (!writeAcknowledgement.getAcknowledged())
            throw new IllegalArgumentException("The 'acknowledged' field of the acknowledgement should be true.");

        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO writeAcknowledgementDTO = null;

        try {
            writeAcknowledgementDTO = session.newInstance(WriteAcknowledgementDTO.class);
            copyState(writeAcknowledgementDTO, writeAcknowledgement);
            session.updatePersistent(writeAcknowledgementDTO); // Throw exception if it does NOT exist.
            LOG.debug("Successfully stored " + writeAcknowledgement.toString());
        } finally {
            session.release(writeAcknowledgementDTO);
        }
    }

    @Override
    public void addWriteAcknowledgement(WriteAcknowledgement writeAcknowledgement) throws StorageException {
        LOG.debug("ADD " + writeAcknowledgement.toString());
        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO writeAcknowledgementDTO = null;

        try {
            writeAcknowledgementDTO = session.newInstance(WriteAcknowledgementDTO.class);
            copyState(writeAcknowledgementDTO, writeAcknowledgement);
            session.makePersistent(writeAcknowledgementDTO); // Throw exception if it exists.
            LOG.debug("Successfully stored " + writeAcknowledgement.toString());
        } finally {
            session.release(writeAcknowledgementDTO);
        }
    }

    @Override
    public void addWriteAcknowledgements(WriteAcknowledgement[] writeAcknowledgements) throws StorageException {
        LOG.debug("ADD " + Arrays.toString(writeAcknowledgements));
        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO[] writeAcknowledgementDTOs = new WriteAcknowledgementDTO[writeAcknowledgements.length];

        try {
            for (int i = 0; i < writeAcknowledgements.length; i++) {
                writeAcknowledgementDTOs[i] = session.newInstance(WriteAcknowledgementDTO.class);
                copyState(writeAcknowledgementDTOs[i], writeAcknowledgements[i]);
            }

            // Throw exception if any exist.
            session.makePersistentAll(Arrays.asList(writeAcknowledgementDTOs));
            LOG.debug("Successfully stored " + Arrays.toString(writeAcknowledgements));
        } finally {
            session.release(writeAcknowledgementDTOs);
        }
    }

    @Override
    public List<WriteAcknowledgement> getWriteAcknowledgements(String operationId) throws StorageException {
        LOG.debug("GET WriteAcknowledgements (operationId=" + operationId + ")");
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(WriteAcknowledgementDTO.class);

        HopsPredicate operationIdPredicate =
                domainType.get("operationId").equal(domainType.param("operationIdParam"));
        domainType.where(operationIdPredicate);

        HopsQuery<WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("operationIdParameter", operationId);

        List<WriteAcknowledgementDTO> dtoResults = query.getResultList();
        List<WriteAcknowledgement> results = new ArrayList<>();

        for (WriteAcknowledgementDTO dto : dtoResults) {
            results.add(convert(dto));
        }

        return results;
    }

    /**
     * Convert the given {@link io.hops.metadata.ndb.dalimpl.hdfs.WriteAcknowledgementClusterJ.WriteAcknowledgementDTO}
     * instance to an object of type {@link io.hops.metadata.hdfs.entity.WriteAcknowledgement}.
     * @param src The WriteAcknowledgementDTO source object.
     * @return An instance of WriteAcknowledgement whose instance variables have been populated from the {@code src}
     * parameter.
     */
    private WriteAcknowledgement convert(WriteAcknowledgementDTO src) {
        return new WriteAcknowledgement(
                src.getNameNodeId(), src.getDeploymentNumber(), src.getOperationId(),
                NdbBoolean.convert(src.getAcknowledged())
        );
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.WriteAcknowledgement} instance to the given
     * {@link io.hops.metadata.ndb.dalimpl.hdfs.WriteAcknowledgementClusterJ.WriteAcknowledgementDTO} instance.
     * @param dest The WriteAcknowledgementDTO destination object.
     * @param src The WriteAcknowledgement source object.
     */
    private void copyState(WriteAcknowledgementDTO dest, WriteAcknowledgement src) {
        dest.setNameNodeId(src.getNameNodeId());
        dest.setAcknowledged(NdbBoolean.convert(src.getAcknowledged()));
        dest.setDeploymentNumber(src.getDeploymentNumber());
        dest.setOperationId(src.getOperationId());
    }
}
