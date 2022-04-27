package org.apache.hadoop.hdfs.serverless.operation.execution.taskarguments;

import com.google.gson.JsonObject;

import java.util.List;

public class JsonTaskArguments implements TaskArguments {
    private final JsonObject taskArguments;

    public JsonTaskArguments(JsonObject taskArguments) {
        this.taskArguments = taskArguments;
    }

    @Override
    public Object get(String key) {
        return taskArguments.getAsJsonPrimitive(key);
    }

    @Override
    public boolean contains(String key) {
        return taskArguments.has(key);
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
