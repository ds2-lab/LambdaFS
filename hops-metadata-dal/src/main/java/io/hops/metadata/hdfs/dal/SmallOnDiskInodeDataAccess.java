package io.hops.metadata.hdfs.dal;

import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;

/**
 * Created by salman on 3/10/16.
 */
public interface SmallOnDiskInodeDataAccess<T> extends DBFileDataAccess<T>, EntityDataAccess {
}

