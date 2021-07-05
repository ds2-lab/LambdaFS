package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

/**
 * Interface defining the API used to read and write DatanodeStorage objects to NDB.
 */
public interface DatanodeStorageDataAccess<T> extends EntityDataAccess {
    /**
     * Find and return the DatanodeStorage instance with the given storageId from NDB.
     */
    T getDatanodeStorage(String storageId) throws StorageException;

    /**
     * Find and remove/delete the DatanodeStorage instance with the given storageId.
     */
    void removeDatanodeStorage(String storageId) throws StorageException;

    /**
     * Store the given DatanodeStorage instance in NDB.
     * @param datanodeStorage The DatanodeStorage instance to store in NDB.
     */
    void addDatanodeStorage(T datanodeStorage) throws StorageException;
}
