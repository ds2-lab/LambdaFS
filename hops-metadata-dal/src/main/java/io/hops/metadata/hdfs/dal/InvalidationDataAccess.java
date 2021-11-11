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
     */
    void addInvalidation(T invalidation) throws StorageException;

    /**
     * Store the given invalidation instances in intermediate storage.
     * @param invalidations The invalidation instances to add.
     */
    void addInvalidations(Collection<T> invalidations) throws StorageException;

    /**
     * Store the given invalidation instances in intermediate storage.
     * @param invalidations The invalidation instances to add.
     */
    void addInvalidations(T[] invalidations) throws StorageException;

    /**
     * Retrieve all invalidations associated with a particular leader/follower NameNode.
     * The specified ID should either be for a Leader NN or a follower NN. Whether it is for a leader or a
     * follower is specified with the boolean parameter.
     *
     * @param nameNodeId The ID of the NameNode in question.
     * @param idIsForTarget If true, then the 'nameNodeId' parameter is taken to be for a target/recipient NN (the
     *                      NN that is supposed to ACK the entry.) If false, then the 'nameNodeId' parameter taken
     *                      to specify the leader NN (the NN that added the entries to intermediate storage).
     */
    List<T> getInvalidations(long nameNodeId, boolean idIsForTarget) throws StorageException;

    /**
     * Remove some invalidations from intermediate storage.
     *
     * @param invalidations The invalidations to delete from intermediate storage.
     */
    void deleteInvalidations(Collection<T> invalidations) throws StorageException;

    /**
     * Remove some invalidations from intermediate storage.
     *
     * @param invalidations The invalidations to delete from intermediate storage.
     */
    void deleteInvalidations(T[] invalidations) throws StorageException;

    /**
     * Remove an invalidation from intermediate storage.
     *
     * @param invalidation The invalidation to delete from intermediate storage.
     */
    void deleteInvalidations(T invalidation) throws StorageException;
}
