package org.apache.hadoop.hdfs.serverless.operation.execution;

public interface TaskArguments {
    Object get(String key);

    boolean contains(String key);
}
