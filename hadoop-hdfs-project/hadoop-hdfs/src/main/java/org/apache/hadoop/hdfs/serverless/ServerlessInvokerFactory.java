package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonObject;
import io.hops.DalStorageFactory;
import io.hops.exception.StorageInitializtionException;

public class ServerlessInvokerFactory {

    /**
     * Return the class name of the serverless invoker for the specified serverless platform.
     *
     * @throws StorageInitializtionException If the specified serverless platform is not supported (i.e., there does
     * not exist a serverless invoker implementation for the specified serverless platform).
     */
    private static String getInvokerClassName(String serverlessPlatformName) throws StorageInitializtionException {
        if (serverlessPlatformName.equals("openwhisk"))
            return "org.apache.hadoop.hdfs.serverless.OpenWhiskInvoker";
        else
            throw new StorageInitializtionException(
                    "Unsupported serverless platform specified: \"" + serverlessPlatformName + "\"");
    }

    /**
     * Return an instance of the specified serverless invoker class.
     */
    public static ServerlessInvoker<JsonObject> getServerlessInvoker(String serverlessPlatformName) throws StorageInitializtionException {
        String invokerClassName = getInvokerClassName(serverlessPlatformName);

        try {
            return (ServerlessInvoker<JsonObject>) Class.forName(invokerClassName).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            throw new StorageInitializtionException(ex);
        }
    }
}
