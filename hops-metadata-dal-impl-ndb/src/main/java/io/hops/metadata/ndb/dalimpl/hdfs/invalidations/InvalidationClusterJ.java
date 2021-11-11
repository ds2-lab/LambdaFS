package io.hops.metadata.ndb.dalimpl.hdfs.invalidations;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.InvalidationDataAccess;
import io.hops.metadata.hdfs.entity.Invalidation;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos.WriteAcknowledgementDTO;
import io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos.InvalidationDTO;
import io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos.InvalidationDeployment0;
import io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos.InvalidationDeployment1;
import io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos.InvalidationDeployment2;
import io.hops.metadata.ndb.wrapper.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class InvalidationClusterJ implements
        InvalidationDataAccess<Invalidation>, TablesDef.WriteAcknowledgementsTableDef {
    private static final Log LOG = LogFactory.getLog(InvalidationClusterJ.class);
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    /**+
     * Static list containing all the ClusterJ @PersistenceCapable interfaces, one for each deployment.
     */
    private static final List<Class<? extends InvalidationDTO>> targetDeployments = Arrays.asList(
            InvalidationDeployment0.class,
            InvalidationDeployment1.class,
            InvalidationDeployment2.class
    );

    /**
     * Return the interface associated with the specified deployment number.
     * @param deploymentNumber The target deployment number.
     */
    private Class<? extends InvalidationDTO> getDTOClass(int deploymentNumber) {
        if (deploymentNumber < 0 || deploymentNumber > targetDeployments.size())
            throw new IllegalArgumentException("Deployment number must be in the range [0," +
                    targetDeployments.size() + "]. Specified value " + deploymentNumber + " is not in this range.");

        return targetDeployments.get(deploymentNumber);
    }

    @Override
    public void addInvalidation(Invalidation invalidation, int deploymentNumber) throws StorageException {
        LOG.debug("ADD " + invalidation.toString() + ", deployment=" + deploymentNumber);

        HopsSession session = connector.obtainSession();
        InvalidationDTO dto = null;
        try {
            dto = session.newInstance(getDTOClass(deploymentNumber));
            copyState(dto, invalidation);
            session.makePersistent(dto);
        } finally {
            session.release(dto);
        }
    }

    @Override
    public void addInvalidations(Collection<Invalidation> invalidations, int deploymentNumber) throws StorageException {
        LOG.debug("ADD " + invalidations.toString() + ", deployment=" + deploymentNumber);
        HopsSession session = connector.obtainSession();
        List<InvalidationDTO> dtos =
                new ArrayList<>();

        try {
            for (Invalidation inv : invalidations) {
                InvalidationDTO dto = session.newInstance(getDTOClass(deploymentNumber));
                copyState(dto, inv);
                dtos.add(dto);
            }

            // Throw exception if any exist.
            session.makePersistentAll(dtos);
            LOG.debug("Successfully stored " + invalidations);
        } finally {
            session.release(dtos);
        }
    }

    @Override
    public void addInvalidations(Invalidation[] invalidations, int deploymentNumber) throws StorageException {
        LOG.debug("ADD " + Arrays.toString(invalidations) + ", deployment=" + deploymentNumber);
        HopsSession session = connector.obtainSession();
        InvalidationDTO[] dtos = new InvalidationDTO[invalidations.length];

        try {
            for (int i = 0; i < invalidations.length; i++) {
                dtos[i] = session.newInstance(getDTOClass(deploymentNumber));
                copyState(dtos[i], invalidations[i]);
            }

            // Throw exception if any exist.
            session.makePersistentAll(Arrays.asList(dtos));
            LOG.debug("Successfully stored " + Arrays.toString(invalidations));
        } finally {
            session.release(dtos);
        }
    }

    @Override
    public List<Invalidation> getInvalidationsForINode(long inodeId, int deploymentNumber) throws StorageException {
        LOG.debug("GET - INode ID=" + inodeId + ", deployment=" + deploymentNumber );

        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<? extends InvalidationDTO> domainType =
                queryBuilder.createQueryDefinition(getDTOClass(deploymentNumber));

        HopsPredicate idPredicate = domainType.get("inodeId").equal(domainType.get("inodeIdParam"));
        domainType.where(idPredicate);
        HopsQuery<? extends InvalidationDTO> query = session.createQuery(domainType);
        query.setParameter("inodeIdParam", inodeId);

        List<? extends InvalidationDTO> dtoResults = query.getResultList();
        List<Invalidation> results = new ArrayList<>();

        for (InvalidationDTO dto : dtoResults) {
            results.add(convert(dto));
        }

        return results;
    }

    @Override
    public void deleteInvalidations(Collection<Invalidation> invalidations, int deploymentNumber) throws StorageException {
        LOG.debug("DELETE - " + invalidations + ", deployment=" + deploymentNumber);
        HopsSession session = connector.obtainSession();
        List<InvalidationDTO> deletions = new ArrayList<>();
        for (Invalidation invalidation : invalidations) {
            Object[] pk = new Object[3];
            pk[0] = invalidation.getINodeId();
            pk[1] = invalidation.getOperationId();
            pk[2] = invalidation.getLeaderNameNodeId();
            InvalidationDTO persistable = session.newInstance(getDTOClass(deploymentNumber), pk);
            deletions.add(persistable);
        }

        session.deletePersistentAll(deletions);
    }

    @Override
    public void deleteInvalidations(Invalidation[] invalidations, int deploymentNumber) throws StorageException {
        LOG.debug("DELETE - " + Arrays.toString(invalidations) + ", deployment=" + deploymentNumber);
        HopsSession session = connector.obtainSession();
        List<InvalidationDTO> deletions = new ArrayList<>();
        for (Invalidation invalidation : invalidations) {
            Object[] pk = new Object[3];
            pk[0] = invalidation.getINodeId();
            pk[1] = invalidation.getOperationId();
            pk[2] = invalidation.getLeaderNameNodeId();
            InvalidationDTO persistable = session.newInstance(getDTOClass(deploymentNumber), pk);
            deletions.add(persistable);
        }

        session.deletePersistentAll(deletions);
    }

    @Override
    public void deleteInvalidation(Invalidation invalidation, int deploymentNumber) throws StorageException {
        LOG.debug("DELETE - " + invalidation + ", deployment=" + deploymentNumber);
        HopsSession session = connector.obtainSession();
        Object[] pk = new Object[3];
        pk[0] = invalidation.getINodeId();
        pk[1] = invalidation.getOperationId();
        pk[2] = invalidation.getLeaderNameNodeId();
        session.deletePersistent(getDTOClass(deploymentNumber), pk);
    }

    /**
     * Convert the given {@link InvalidationDTO}
     * instance to an object of type {@link io.hops.metadata.hdfs.entity.Invalidation}.
     * @param src The InvalidationDTO source object.
     * @return An instance of Invalidation whose instance variables have been populated from the {@code src}
     * parameter.
     */
    private Invalidation convert(InvalidationDTO src) {
        // int inodeId, int parentId, long leaderNameNodeId, long txStartTime, long operationId
        return new Invalidation(
            src.getINodeId(), src.getParentId(), src.getLeaderId(), src.getTxStart(), src.getOperationId());
    }

    /**
     * Copy the state from the given {@link io.hops.metadata.hdfs.entity.Invalidation} instance to the given
     * {@link InvalidationDTO} instance.
     * @param dest The InvalidationDTO destination object.
     * @param src The Invalidation source object.
     */
    private void copyState(InvalidationDTO dest, Invalidation src) {
        dest.setOperationId(src.getOperationId());
        dest.setLeaderId(src.getLeaderNameNodeId());
        dest.setParentId(src.getParentId());
        dest.setTxStart(src.getTxStartTime());
        dest.setINodeId(src.getINodeId());
    }
}
