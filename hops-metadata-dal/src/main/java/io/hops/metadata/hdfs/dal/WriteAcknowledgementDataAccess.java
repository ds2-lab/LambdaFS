package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;

import java.util.Collection;
import java.util.List;

/**
 * Defines the interface for storing and retrieving WriteAcknowledgements from intermediate storage.
 */
public interface WriteAcknowledgementDataAccess<T> extends EntityDataAccess {
    /**
     * Retrieve the WriteAcknowledgement entry from the specified write operation, corresponding to the specified NN.
     * @param nameNodeId ID of the NN.
     * @param operationId ID of the write operation.
     * @return The WriteAcknowledgement entry.
     */
    T getWriteAcknowledgement(long nameNodeId, long operationId) throws StorageException;

    /**
     * Store the given write acknowledgement in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgement The WriteAcknowledgement to store in intermediate storage.
     */
    void addWriteAcknowledgement(T writeAcknowledgement) throws StorageException;

    /**
     * Acknowledge the given write acknowledgement. The parameterized instance's 'acknowledged' field should be true,
     * or an exception will be thrown. Also throws an exception if there does not exist an entry already in
     * intermediate storage for this ACK.
     */
    void acknowledge(T writeAcknowledgement) throws StorageException;

    /**
     * Delete a WriteAcknowledgement instance from intermediate storage.
     *
     * TODO: What if follower NN simply deleted their entries, rather than ACK-ing them?
     * @param writeAcknowledgement The instance to delete.
     */
    void deleteAcknowledgement(T writeAcknowledgement) throws StorageException;

    /**
     * Delete a collection of WriteAcknowledgement instances from intermediate storage.
     *
     * TODO: What if follower NN simply deleted their entries, rather than ACK-ing them?
     * @param writeAcknowledgements The instances to delete.
     */
    void deleteAcknowledgements(Collection<T> writeAcknowledgements) throws StorageException;

    /**
     * Store the given write acknowledgements in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgements Array of WriteAcknowledgements to store in intermediate storage.
     */
    void addWriteAcknowledgements(T[] writeAcknowledgements) throws StorageException;

    /**
     * Store the given write acknowledgements in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgements Array of WriteAcknowledgements to store in intermediate storage.
     */
    void addWriteAcknowledgements(Collection<T> writeAcknowledgements) throws StorageException;

    /**
     * Retrieve all write acknowledgements created for the specified write operation.
     * @param operationId ID of the write operation.
     * @return Array of write acknowledgements from the specified operation.
     */
    List<T> getWriteAcknowledgements(long operationId) throws StorageException;
}
