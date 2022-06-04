package org.apache.hadoop.hdfs.serverless.execution.taskarguments;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hdfs.serverless.invoking.InvokerUtilities;

import java.util.ArrayList;
import java.util.List;

public class JsonTaskArguments implements TaskArguments {
    private JsonObject taskArguments;

    public JsonTaskArguments(JsonObject taskArguments) {
        this.taskArguments = taskArguments;
    }

    private JsonTaskArguments() { }

    @Override
    public boolean contains(String key) {
        return taskArguments.has(key);
    }

    @Override
    public String getString(String key) {
        if (taskArguments.has(key))
            return taskArguments.getAsJsonPrimitive(key).getAsString();
        return null;
    }

    @Override
    public <T> T getObject(String key) {
        if (!taskArguments.has(key))
            return null;

        String base64Encoded = taskArguments.getAsJsonPrimitive(key).getAsString();
        return (T) InvokerUtilities.base64StringToObject(base64Encoded);
    }

    @Override
    public byte[] getByteArray(String key) {
        if (taskArguments.has(key)) {
            String base64Encoded = taskArguments.getAsJsonPrimitive(key).getAsString();
            return Base64.decodeBase64(base64Encoded);
        }

        return null;
    }

    @Override
    public long getLong(String key) {
        if (taskArguments.has(key))
            return taskArguments.getAsJsonPrimitive(key).getAsLong();

        throw new IllegalArgumentException("Task Arguments do not contain entry for key '" + key + "'");
    }

    @Override
    public short getShort(String key) {
        if (taskArguments.has(key))
            return taskArguments.getAsJsonPrimitive(key).getAsShort();

        throw new IllegalArgumentException("Task Arguments do not contain entry for key '" + key + "'");
    }

    @Override
    public <T> List<T> getList(String key) {
        if (!taskArguments.has(key))
            return null;

        List<T> list = new ArrayList<T>();
        JsonArray jsonArray = taskArguments.getAsJsonArray(key);
        for (int i = 0; i < jsonArray.size(); i++) {
            list.add((T)InvokerUtilities.base64StringToObject(jsonArray.get(i).getAsString()));
        }
        return list;
    }

    @Override
    public List<String> getStringList(String key) {
        if (!taskArguments.has(key))
            return null;

        List<String> list = new ArrayList<String>();
        JsonArray jsonArray = taskArguments.getAsJsonArray(key);
        for (int i = 0; i < jsonArray.size(); i++)
            list.add(jsonArray.get(i).getAsString());

        return list;
    }

    @Override
    public String[] getStringArray(String key) {
        if (!taskArguments.has(key))
            return null;

        JsonArray jsonArray = taskArguments.getAsJsonArray(key);
        String[] arr = new String[jsonArray.size()];
        for (int i = 0; i < jsonArray.size(); i++)
            arr[i] = jsonArray.get(i).getAsString();

        return arr;
    }

    @Override
    public <T> T[] getObjectArray(String key) {
        if (!taskArguments.has(key))
            throw new IllegalArgumentException("Task Arguments do not contain entry for key '" + key + "'");

        JsonArray jsonArray = taskArguments.getAsJsonArray(key);
        Object[] arr = new Object[jsonArray.size()];
        for (int i = 0; i < jsonArray.size(); i++) {
            arr[i] = InvokerUtilities.base64StringToObject(jsonArray.get(i).getAsString());
        }

        return (T[])arr;
    }

    @Override
    public int getInt(String key) {
        if (taskArguments.has(key))
            return taskArguments.getAsJsonPrimitive(key).getAsInt();

        throw new IllegalArgumentException("Task Arguments do not contain entry for key '" + key + "'");
    }

    @Override
    public boolean getBoolean(String key) {
        if (taskArguments.has(key))
            return taskArguments.getAsJsonPrimitive(key).getAsBoolean();

        throw new IllegalArgumentException("Task Arguments do not contain entry for key '" + key + "'");
    }
}
