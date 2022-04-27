package org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments;

import java.util.List;

/**
 * Interface that enables us to transparently use {@link com.google.gson.JsonObject} and {@link java.util.HashMap}
 * objects to deliver the arguments to the file system operations.
 */
public interface TaskArguments {
    Object get(String key);

    boolean contains(String key);

    String getString(String key);

    <T> T getObject(String key);

    long getLong(String key);

    <T> List<T> getList(String key);

    <T> T[] getArray(String key);

    int getInt(String key);

    boolean getBoolean(String key);
}
