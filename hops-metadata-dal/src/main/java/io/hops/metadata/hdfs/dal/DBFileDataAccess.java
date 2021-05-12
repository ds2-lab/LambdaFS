package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

/**
 * Created by salman on 4/28/17.
 */
  public interface DBFileDataAccess<T> extends EntityDataAccess {
    void add(T fileInodeData) throws StorageException;

    T get(long inodeId) throws StorageException;

    void delete(T fileInodeData) throws StorageException;

    int count() throws StorageException;

    int getLength() throws StorageException;
  }

