package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

import java.util.Collection;
import java.util.List;

/**
 * Defines general interface for working with write invalidations.
 */
public interface InvalidationDataAccess<T> extends EntityDataAccess {
    /**
     * Store the given invalidation instance in intermediate storage.
     * @param invalidation The invalidation instance to add.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addInvalidation(T invalidation, int deploymentNumber) throws StorageException;

    /**
     * Store the given invalidation instances in intermediate storage.
     * @param invalidations The invalidation instances to add.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addInvalidations(Collection<T> invalidations, int deploymentNumber) throws StorageException;

    /**
     * Store the given invalidation instances in intermediate storage.
     * @param invalidations The invalidation instances to add.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addInvalidations(T[] invalidations, int deploymentNumber) throws StorageException;

    /**
     * Retrieve all invalidations associated with a particular leader/follower NameNode.
     *
     * @param inodeId The ID of the INode that got invalidated.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    List<T> getInvalidationsForINode(long inodeId, int deploymentNumber) throws StorageException;

    /**
     * Remove some invalidations from intermediate storage.
     *
     * @param invalidations The invalidations to delete from intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void deleteInvalidations(Collection<T> invalidations, int deploymentNumber) throws StorageException;

    /**
     * Remove some invalidations from intermediate storage.
     *
     * @param invalidations The invalidations to delete from intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void deleteInvalidations(T[] invalidations, int deploymentNumber) throws StorageException;

    /**
     * Remove an invalidation from intermediate storage.
     *
     * @param invalidation The invalidation to delete from intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void deleteInvalidation(T invalidation, int deploymentNumber) throws StorageException;
}
