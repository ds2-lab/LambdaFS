package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;

public class JsonObjectWithSubtreeFlag {
    private final JsonObject object;
    private final boolean containsSubtreeOperation;

    public JsonObjectWithSubtreeFlag(JsonObject obj, boolean containsSubtreeOperation) {
        this.object = obj;
        this.containsSubtreeOperation = containsSubtreeOperation;
    }

    public boolean getContainsSubtreeOp() {
        return this.containsSubtreeOperation;
    }

    public JsonObject getObject() {
        return this.object;
    }
}
