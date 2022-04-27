package org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments;

import java.util.HashMap;
import java.util.List;

public class HashMapTaskArguments implements TaskArguments {
    private final HashMap<String, Object> taskArguments;

    public HashMapTaskArguments(HashMap<String, Object> taskArguments) {
        this.taskArguments = taskArguments;
    }

    @Override
    public Object get(String key) {
        return taskArguments.get(key);
    }

    @Override
    public boolean contains(String key) {
        return taskArguments.containsKey(key);
    }

    @Override
    public String getString(String key) {

    }

    @Override
    public <T> T[] getArray(String key) {

    }

    @Override
    public <T> T getObject(String key) {

    }

    @Override
    public long getLong(String key) {

    }

    @Override
    public <T> List<T> getList(String key) {

    }

    @Override
    public int getInt(String key) {

    }

    @Override
    public boolean getBoolean(String key) {

    }
}
