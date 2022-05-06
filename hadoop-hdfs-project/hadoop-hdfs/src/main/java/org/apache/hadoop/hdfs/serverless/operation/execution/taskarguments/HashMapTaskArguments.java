package org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments;

import com.google.gson.JsonArray;
import org.apache.commons.codec.binary.Base64;

import java.util.HashMap;
import java.util.List;

public class HashMapTaskArguments implements TaskArguments {
    private HashMap<String, Object> taskArguments;

    public HashMapTaskArguments(HashMap<String, Object> taskArguments) {
        this.taskArguments = taskArguments;
    }

    private HashMapTaskArguments() { }

    @Override
    public boolean contains(String key) {
        return taskArguments.containsKey(key);
    }

    @Override
    public String getString(String key) {
        return (String)taskArguments.get(key);
    }

    @Override
    public <T> T[] getObjectArray(String key) {
        return getObject(key);
    }

    @Override
    public <T> T getObject(String key) {
        return (T)taskArguments.get(key);
    }

    @Override
    public byte[] getByteArray(String key) {
        return getObject(key);
    }

    @Override
    public long getLong(String key) {
        return (long)taskArguments.get(key);
    }

    @Override
    public <T> List<T> getList(String key) {
        return getObject(key);
    }

    @Override
    public List<String> getStringList(String key) {
        return getList(key);
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
