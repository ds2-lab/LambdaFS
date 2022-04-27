package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.google.gson.JsonObject;

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
}
