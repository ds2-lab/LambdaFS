package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Defines the interface for storing and retrieving WriteAcknowledgements from intermediate storage.
 */
public interface WriteAcknowledgementDataAccess<T> extends EntityDataAccess {
    /**
     * Retrieve the WriteAcknowledgement entry from the specified write operation, corresponding to the specified NN.
     * @param nameNodeId ID of the NN.
     * @param operationId ID of the write operation.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     * @return The WriteAcknowledgement entry.
     */
    T getWriteAcknowledgement(long nameNodeId, long operationId, int deploymentNumber) throws StorageException;

    /**
     * Check if there are any pending ACKs for the NameNode specified by the given ID. A pending ACK is one which has
     * not yet been acknowledged.
     *
     * @param nameNodeId The NameNode for which to check for pending ACKs.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     *
     * @return A mapping from operationId to the WriteAcknowledgement object for all pending ACKs. That is, for each
     * pending ACK for the NameNode identified by the given ID, an entry will be added to the map where the key is the
     * ACK's write operation ID and the value is the WriteAcknowledgement object.
     * @throws StorageException
     */
    List<T> getPendingAcks(long nameNodeId, int deploymentNumber) throws StorageException;

    /**
     * Check if there are any pending ACKs for the NameNode specified by the given ID. A pending ACK is one which has
     * not yet been acknowledged.
     *
     * @param nameNodeId The NameNode for which to check for pending ACKs.
     * @param minTime The write operation associated with the ACKs must have started at this time or LATER.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     *
     * @return A mapping from operationId to the WriteAcknowledgement object for all pending ACKs. That is, for each
     * pending ACK for the NameNode identified by the given ID, an entry will be added to the map where the key is the
     * ACK's write operation ID and the value is the WriteAcknowledgement object.
     * @throws StorageException
     */
    List<T> getPendingAcks(long nameNodeId, long minTime, int deploymentNumber) throws StorageException;

    /**
     * Store the given write acknowledgement in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgement The WriteAcknowledgement to store in intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addWriteAcknowledgement(T writeAcknowledgement, int deploymentNumber) throws StorageException;

    /**
     * Acknowledge the given write acknowledgement. The parameterized instance's 'acknowledged' field should be true,
     * or an exception will be thrown. Also throws an exception if there does not exist an entry already in
     * intermediate storage for this ACK.
     *
     * @param writeAcknowledgement The pending entry to ACK.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void acknowledge(T writeAcknowledgement, int deploymentNumber) throws StorageException;

    /**
     * Acknowledge the given write acknowledgement. The parameterized instance's 'acknowledged' field should be true,
     * or an exception will be thrown. Also throws an exception if there does not exist an entry already in
     * intermediate storage for this ACK.
     *
     * @param writeAcknowledgements List of pending entries to ACK.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void acknowledge(List<T> writeAcknowledgements, int deploymentNumber) throws StorageException;

    /**
     * Delete a WriteAcknowledgement instance from intermediate storage.
     *
     * @param writeAcknowledgement The instance to delete.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void deleteAcknowledgement(T writeAcknowledgement, int deploymentNumber) throws StorageException;

    /**
     * Delete a collection of WriteAcknowledgement instances from intermediate storage.
     *
     * @param writeAcknowledgements The instances to delete.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void deleteAcknowledgements(Collection<T> writeAcknowledgements, int deploymentNumber) throws StorageException;

    /**
     * Store the given write acknowledgements in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgements Array of WriteAcknowledgements to store in intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addWriteAcknowledgements(T[] writeAcknowledgements, int deploymentNumber) throws StorageException;

    /**
     * Store the given write acknowledgements in intermediate storage.
     *
     * NOTE: This should only be used for adding the un-ACK'd entries. Use the separate API for ACK-ing an entry.
     * @param writeAcknowledgements Array of WriteAcknowledgements to store in intermediate storage.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     */
    void addWriteAcknowledgements(Collection<T> writeAcknowledgements, int deploymentNumber) throws StorageException;

    /**
     * Retrieve all write acknowledgements created for the specified write operation.
     * @param operationId ID of the write operation.
     * @param deploymentNumber The deployment number of the NameNodes we're working with.
     * @return Array of write acknowledgements from the specified operation.
     */
    List<T> getWriteAcknowledgements(long operationId, int deploymentNumber) throws StorageException;
}