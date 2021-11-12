package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos.WriteAcknowledgementDTO;
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos.WriteAcknowledgementDeployment0;
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos.WriteAcknowledgementDeployment1;
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos.WriteAcknowledgementDeployment2;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class WriteAcknowledgementClusterJ
        implements WriteAcknowledgementDataAccess<WriteAcknowledgement>, TablesDef.WriteAcknowledgementsTableDef {
    private static final Log LOG = LogFactory.getLog(WriteAcknowledgementClusterJ.class);
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    /**
     * Static list containing all the ClusterJ @PersistenceCapable interfaces, one for each deployment.
     */
    private static final List<Class<? extends WriteAcknowledgementDTO>> targetDeployments = Arrays.asList(
            WriteAcknowledgementDeployment0.class,
            WriteAcknowledgementDeployment1.class,
            WriteAcknowledgementDeployment2.class
    );

    /**
     * Return the interface associated with the specified deployment number.
     * @param deploymentNumber The target deployment number.
     */
    private Class<? extends WriteAcknowledgementDTO> getDTOClass(int deploymentNumber) {
        if (deploymentNumber < 0 || deploymentNumber > targetDeployments.size())
            throw new IllegalArgumentException("Deployment number must be in the range [0," +
                    targetDeployments.size() + "]. Specified value " + deploymentNumber + " is not in this range.");

        return targetDeployments.get(deploymentNumber);
    }

    @Override
    public WriteAcknowledgement getWriteAcknowledgement(long nameNodeId, long operationId, int deploymentNumber)
            throws StorageException {
        LOG.debug("GET WriteAcknowledgement (nameNodeId=" + nameNodeId + ", operationId=" + operationId + ")");
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<? extends WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(getDTOClass(deploymentNumber));

        // We want to match against nameNodeId and operationId.
        HopsPredicate nameNodeIdPredicate =
                domainType.get("nameNodeId").equal(domainType.param("nameNodeIdParam"));
        HopsPredicate operationIdPredicate =
                domainType.get("operationId").equal(domainType.param("operationIdParam"));

        // We need to match against BOTH, not just one.
        domainType.where(nameNodeIdPredicate.and(operationIdPredicate));

        // Create the query.
        HopsQuery<? extends WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("nameNodeIdParam", nameNodeId);
        query.setParameter("operationIdParam", operationId);

        List<? extends WriteAcknowledgementDTO> results = query.getResultList();
        WriteAcknowledgement writeAcknowledgement = null;
        if (results.size() == 1) {
            WriteAcknowledgementDTO writeAckDTO = results.get(0);
            writeAcknowledgement = convert(writeAckDTO);
        }

        session.release(results);
        return writeAcknowledgement;
    }

    @Override
    public List<WriteAcknowledgement> getPendingAcks(long nameNodeId, int deploymentNumber)
            throws StorageException {
        LOG.debug("CHECK PENDING ACKS - ID=" + nameNodeId);
        List<WriteAcknowledgement> pendingAcks = new ArrayList<>();
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<? extends WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(getDTOClass(deploymentNumber));

        HopsPredicate nameNodeIdPredicate =
                domainType.get("nameNodeId").equal(domainType.param("nameNodeIdParam"));
        domainType.where(nameNodeIdPredicate);

        HopsQuery<? extends WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("nameNodeIdParam", nameNodeId);

        List<? extends WriteAcknowledgementDTO> dtoResults = query.getResultList();

        for (WriteAcknowledgementDTO dto : dtoResults) {
            if (!NdbBoolean.convert(dto.getAcknowledged()))
                pendingAcks.add(convert(dto));
        }

        return pendingAcks;
    }

    // Only returns ACKs with an associated TX start-time >= the 'minTime' parameter.
    @Override
    public List<WriteAcknowledgement> getPendingAcks(long nameNodeId, long minTime, int deploymentNumber)
            throws StorageException {
        LOG.debug("CHECK PENDING ACKS - ID=" + nameNodeId + ", MinTime=" + minTime);
        List<WriteAcknowledgement> pendingAcks = new ArrayList<>();
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<? extends WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(getDTOClass(deploymentNumber));

        HopsPredicate nameNodeIdPredicate =
                domainType.get("nameNodeId").equal(domainType.param("nameNodeIdParam"));
        HopsPredicate timePredicate =
                domainType.get("timestamp").greaterEqual(domainType.param("timestampParameter"));
        domainType.where(nameNodeIdPredicate.and(timePredicate));

        HopsQuery<? extends WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("nameNodeIdParam", nameNodeId);
        query.setParameter("timestampParameter", minTime);

        List<? extends WriteAcknowledgementDTO> dtoResults = query.getResultList();

        // There should just be one WriteAcknowledgementDTO object per operation since all of these ACKs
        // are for our local NN, and each operation would require an ACK from our local NN at most once.
        for (WriteAcknowledgementDTO dto : dtoResults) {
            if (!NdbBoolean.convert(dto.getAcknowledged()))
                pendingAcks.add(convert(dto));
        }

        return pendingAcks;
    }

    @Override
    public void acknowledge(WriteAcknowledgement writeAcknowledgement, int deploymentNumber)
            throws StorageException {
        LOG.debug("ACK " + writeAcknowledgement.toString());

        writeAcknowledgement.acknowledge();

        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO writeDTO = null;

        try {
            writeDTO = session.newInstance(getDTOClass(deploymentNumber));
            copyState(writeDTO, writeAcknowledgement);
            session.updatePersistent(writeDTO); // Throw exception if it does NOT exist.
            LOG.debug("Successfully stored " + writeAcknowledgement.toString());
        } finally {
            session.release(writeDTO);
        }
    }

    @Override
    public void acknowledge(List<WriteAcknowledgement> writeAcknowledgements, int deploymentNumber)
            throws StorageException {
        LOG.debug("ACK " + writeAcknowledgements.toString());

        WriteAcknowledgementDTO[] dtos = new WriteAcknowledgementDTO[writeAcknowledgements.size()];
        HopsSession session = connector.obtainSession();

        for (int i = 0; i < writeAcknowledgements.size(); i++) {
            WriteAcknowledgement ack = writeAcknowledgements.get(i);
            ack.acknowledge();
            dtos[i] = session.newInstance(getDTOClass(deploymentNumber));
            copyState(dtos[i], ack);
        }

        try {
            session.updatePersistentAll(writeAcknowledgements);
        } finally {
            session.release(dtos);
        }
    }

    @Override
    public void deleteAcknowledgement(WriteAcknowledgement writeAcknowledgement, int deploymentNumber)
            throws StorageException {
        LOG.debug("DELETE " + writeAcknowledgement.toString());
        HopsSession session = connector.obtainSession();
        Object[] pk = new Object[3];
        pk[0] = writeAcknowledgement.getNameNodeId();
        pk[1] = writeAcknowledgement.getOperationId();
        pk[2] = writeAcknowledgement.getLeaderNameNodeId();
        session.deletePersistent(getDTOClass(deploymentNumber), pk);
    }

    @Override
    public void deleteAcknowledgements(Collection<WriteAcknowledgement> writeAcknowledgements,
                                       int deploymentNumber)
            throws StorageException {
        LOG.debug("DELETE ALL " + writeAcknowledgements.toString());
        HopsSession session = connector.obtainSession();

        List<WriteAcknowledgementDTO> deletions = new ArrayList<>();
        for (WriteAcknowledgement writeAcknowledgement : writeAcknowledgements) {
            Object[] pk = new Object[3];
            pk[0] = writeAcknowledgement.getNameNodeId();
            pk[1] = writeAcknowledgement.getOperationId();
            pk[2] = writeAcknowledgement.getLeaderNameNodeId();
            WriteAcknowledgementDTO persistable = session.newInstance(getDTOClass(deploymentNumber), pk);
            deletions.add(persistable);
        }

        session.deletePersistentAll(deletions);
    }

    @Override
    public void addWriteAcknowledgement(WriteAcknowledgement writeAcknowledgement, int deploymentNumber)
            throws StorageException {
        LOG.debug("ADD " + writeAcknowledgement.toString());
        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO writeDTO = null;

        try {
            writeDTO = session.newInstance(getDTOClass(deploymentNumber));
            copyState(writeDTO, writeAcknowledgement);
            session.makePersistent(writeDTO); // Throw exception if it exists.
            LOG.debug("Successfully stored " + writeAcknowledgement.toString());
        } finally {
            session.release(writeDTO);
        }
    }

    @Override
    public void addWriteAcknowledgements(WriteAcknowledgement[] writeAcknowledgements, int deploymentNumber)
            throws StorageException {
        LOG.debug("ADD " + Arrays.toString(writeAcknowledgements));
        HopsSession session = connector.obtainSession();

        WriteAcknowledgementDTO[] writeDTOs = new WriteAcknowledgementDTO[writeAcknowledgements.length];

        try {
            for (int i = 0; i < writeAcknowledgements.length; i++) {
                writeDTOs[i] = session.newInstance(getDTOClass(deploymentNumber));
                copyState(writeDTOs[i], writeAcknowledgements[i]);
            }

            // Throw exception if any exist.
            session.makePersistentAll(Arrays.asList(writeDTOs));
            LOG.debug("Successfully stored " + Arrays.toString(writeAcknowledgements));
        } finally {
            session.release(writeDTOs);
        }
    }

    @Override
    public void addWriteAcknowledgements(Collection<WriteAcknowledgement> writeAcknowledgements,
                                         int deploymentNumber)
            throws StorageException {
        LOG.debug("ADD " + StringUtils.join(writeAcknowledgements, " ; "));
        HopsSession session = connector.obtainSession();

        List<WriteAcknowledgementDTO> writeDTOs =
                new ArrayList<>();

        try {
            for (WriteAcknowledgement ack : writeAcknowledgements) {
                WriteAcknowledgementDTO dto = session.newInstance(getDTOClass(deploymentNumber));
                copyState(dto, ack);
                writeDTOs.add(dto);
            }

            // Throw exception if any exist.
            session.makePersistentAll(writeDTOs);
            LOG.debug("Successfully stored " + writeAcknowledgements);
        } finally {
            session.release(writeDTOs);
        }
    }

    @Override
    public List<WriteAcknowledgement> getWriteAcknowledgements(long operationId, int deploymentNumber)
            throws StorageException {
        LOG.debug("GET WriteAcknowledgements (operationId=" + operationId + ")");
        HopsSession session = connector.obtainSession();

        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<? extends WriteAcknowledgementDTO> domainType =
                queryBuilder.createQueryDefinition(getDTOClass(deploymentNumber));

        HopsPredicate operationIdPredicate =
                domainType.get("operationId").equal(domainType.param("operationIdParam"));
        domainType.where(operationIdPredicate);

        HopsQuery<? extends WriteAcknowledgementDTO> query = session.createQuery(domainType);
        query.setParameter("operationIdParameter", operationId);

        List<? extends WriteAcknowledgementDTO> dtoResults = query.getResultList();
        List<WriteAcknowledgement> results = new ArrayList<>();

        for (WriteAcknowledgementDTO dto : dtoResults) {
            results.add(convert(dto));
        }

        return results;
    }

    /**
     * Convert the given {@link WriteAcknowledgementDTO}
     * instance to an object of type {@link io.hops.metadata.hdfs.entity.WriteAcknowledgement}.
     * @param src The WriteAcknowledgementDTO source object.
     * @return An instance of WriteAcknowledgement whose instance variables have been populated from the {@code src}
     * parameter.
     */
    private WriteAcknowledgement convert(WriteAcknowledgementDTO src) {
        return new WriteAcknowledgement(
                src.getNameNodeId(), src.getDeploymentNumber(), src.getOperationId(),
                NdbBoolean.convert(src.getAcknowledged()), src.getTimestamp(), src.getLeaderId());
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.WriteAcknowledgement} instance to the given
     * {@link WriteAcknowledgementDTO} instance.
     * @param dest The WriteAcknowledgementDTO destination object.
     * @param src The WriteAcknowledgement source object.
     */
    private void copyState(WriteAcknowledgementDTO dest, WriteAcknowledgement src) {
        dest.setNameNodeId(src.getNameNodeId());
        dest.setAcknowledged(NdbBoolean.convert(src.getAcknowledged()));
        dest.setDeploymentNumber(src.getDeploymentNumber());
        dest.setOperationId(src.getOperationId());
        dest.setTimestamp(src.getTimestamp());
    }
}
