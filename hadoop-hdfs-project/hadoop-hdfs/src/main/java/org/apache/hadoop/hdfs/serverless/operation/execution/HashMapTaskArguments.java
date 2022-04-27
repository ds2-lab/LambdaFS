package org.apache.hadoop.hdfs.serverless.operation.execution;

import java.util.HashMap;

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
}
