package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.entity.DataNodeMeta;
import io.hops.metadata.hdfs.entity.ServerlessNameNodeMeta;

import java.util.List;

/**
 * Interface defining functions with which to get and remove Serverless NameNodes from the intermediate storage.
 */
public interface ServerlessNameNodeDataAccess<T> extends EntityDataAccess {
    /**
     * Retrieve the Serverless NameNode identified by both its unique ID and the specified
     * serverless function name of the serverless function on which it is theoretically executing.
     * @param nameNodeId The unique ID of the desired NameNode.
     * @param functionName Name of the serverless function on which the NN is running.
     * @return Desired NameNode instance.
     */
    T getServerlessNameNode(long nameNodeId, String functionName) throws StorageException;

    /**
     * Retrieve the Serverless NameNode identified by its unique ID.
     * @param nameNodeId The unique ID of the desired NameNode.
     * @return The desired NameNode.
     */
    T getServerlessNameNodeByNameNodeId(long nameNodeId) throws StorageException;

    /**
     * Retrieve the Serverless NameNode currently associated with the specified function name.
     * @param functionName The name of the serverless function on which the NN is running.
     * @return The NameNode instance that is, in theory, currently running within the specified function.
     */
    T getServerlessNameNodeByFunctionName(String functionName) throws StorageException;

    /**
     * Add a new Serverless NameNode to intermediate storage.
     * @param nameNode The NameNode to add to storage.
     */
    void addServerlessNameNode(T nameNode) throws StorageException;

    /**
     * Replace an existing Serverless NameNode entry with a new one. If no existing entry exists,
     * then the new one is added without replacing anything.
     *
     * The serverless function name is used to determine what gets replaced.
     *
     * @param nameNode The new Serverless NameNode metadata to write to NDB.
     */
    void replaceServerlessNameNode(T nameNode) throws StorageException;

    /**
     * Remove the given serverless name node instance from intermediate storage.
     *
     * This function uses the NameNode's ID to field and delete it.
     */
    void removeServerlessNameNode(T nameNode) throws StorageException;

    /**
     * Remove the given serverless name node instance (which is identified by the name of the serverless
     * function on which it is running) from intermediate storage.
     */
    void removeServerlessNameNode(String functionName) throws StorageException;

    /**
     * Return a list of all Serverless NameNodes stored in intermediate storage.
     */
    List<T> getAllServerlessNameNodes() throws StorageException;
}
