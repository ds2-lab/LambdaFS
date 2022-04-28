package org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments;

import com.google.gson.JsonArray;

import java.util.HashMap;
import java.util.List;

public class HashMapTaskArguments implements TaskArguments {
    private final HashMap<String, Object> taskArguments;

    public HashMapTaskArguments(HashMap<String, Object> taskArguments) {
        this.taskArguments = taskArguments;
    }

    @Override
    public boolean contains(String key) {
        return taskArguments.containsKey(key);
    }

    @Override
    public String getString(String key) {
        return (String)taskArguments.get(key);
    }

    @Override
    public <T> T[] getArray(String key) {
        return (T[])taskArguments.get(key);
    }

    @Override
    public <T> T getObject(String key) {
        return (T)taskArguments.get(key);
    }

    @Override
    public long getLong(String key) {
        return (long)taskArguments.get(key);
    }

    @Override
    public <T> List<T> getList(String key) {
        return (List<T>)taskArguments.get(key);
    }

    @Override
    public int getInt(String key) {
        return (int)taskArguments.get(key);
    }

    @Override
    public short getShort(String key) {
        return (short)taskArguments.get(key);
    }

    @Override
    public String[] getStringArray(String key) {
        return (String[])taskArguments.get(key);
    }

    @Override
    public boolean getBoolean(String key) {
        return (boolean)taskArguments.get(key);
    }
}
