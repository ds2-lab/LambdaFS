package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonObject;
import io.hops.DalStorageFactory;
import io.hops.exception.StorageInitializtionException;

public class ServerlessInvokerFactory {

    /**
     * Return an instance of the specified serverless invoker class.
     */
    public static ServerlessInvoker<JsonObject> getServerlessInvoker(String invokerClassName) throws StorageInitializtionException {
        try {
            return (ServerlessInvoker<JsonObject>) Class.forName(invokerClassName).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            throw new StorageInitializtionException(ex);
        }
    }
}
