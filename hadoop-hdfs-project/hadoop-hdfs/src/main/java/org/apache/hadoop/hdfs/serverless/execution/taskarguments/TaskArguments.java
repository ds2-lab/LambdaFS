package org.apache.hadoop.hdfs.serverless.execution.taskarguments;

import java.util.List;

/**
 * Interface that enables us to transparently use {@link com.google.gson.JsonObject} and {@link java.util.HashMap}
 * objects to deliver the arguments to the file system operations.
 */
public interface TaskArguments {
    boolean contains(String key);

    String getString(String key);

    <T> T getObject(String key);

    long getLong(String key);

    <T> List<T> getList(String key);

    <T> T[] getObjectArray(String key);

    String[] getStringArray(String key);

    byte[] getByteArray(String key);

    int getInt(String key);

    short getShort(String key);

    boolean getBoolean(String key);

    List<String> getStringList(String key);
}
