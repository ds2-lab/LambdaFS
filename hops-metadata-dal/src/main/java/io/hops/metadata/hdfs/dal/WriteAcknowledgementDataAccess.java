package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

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
    T getWriteAcknowledgement(long nameNodeId, String operationId) throws StorageException;

    /**
     * Store the given write acknowledgement in intermediate storage.
     * @param writeAcknowledgement The WriteAcknowledgement to store in intermediate storage.
     */
    void addWriteAcknowledgement(T writeAcknowledgement) throws StorageException;

    /**
     * Store the given write acknowledgements in intermediate storage.
     * @param writeAcknowledgements Array of WriteAcknowledgements to store in intermediate storage.
     */
    void addWriteAcknowledgements(T[] writeAcknowledgements) throws StorageException;

    /**
     * Retrieve all write acknowledgements created for the specified write operation.
     * @param operationId ID of the write operation.
     * @return Array of write acknowledgements from the specified operation.
     */
    List<T> getWriteAcknowledgements(String operationId) throws StorageException;
}
