package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import io.hops.exception.StorageInitializtionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Locale;

/**
 * The serverless platform being used is specified in the configuration files for Serverless HopsFS. Currently, it
 * defaults to OpenWhisk. In order to obtain an invoker, you simply utilize the {@link ServerlessInvokerBase} class,
 * passing whatever platform is specified in the configuration. The factory will provide a concrete implementation
 * for the platform being used.
 */
public class ServerlessInvokerFactory {
    private static final Log LOG = LogFactory.getLog(ServerlessInvokerFactory.class);

    /**
     * Return the class name of the serverless invoker for the specified serverless platform.
     *
     * This automatically checks against the all-lowercase version of the `serverlessPlatformName` parameter.
     *
     * @throws StorageInitializtionException If the specified serverless platform is not supported (i.e., there does
     * not exist a serverless invoker implementation for the specified serverless platform).
     */
    private static String getInvokerClassName(String serverlessPlatformName) throws StorageInitializtionException {
        if (serverlessPlatformName.toLowerCase(Locale.ROOT).equals("openwhisk") ||
                serverlessPlatformName.toLowerCase(Locale.ROOT).equals("open whisk") ||
                serverlessPlatformName.equalsIgnoreCase("nuclio"))
            return "org.apache.hadoop.hdfs.serverless.invoking.OpenWhiskInvoker";
        //else if (serverlessPlatformName.equalsIgnoreCase("nuclio"))
        //    return "org.apache.hadoop.hdfs.serverless.invoking.NuclioInvoker";
        else
            throw new StorageInitializtionException(
                    "Unsupported serverless platform specified: \"" + serverlessPlatformName + "\"");
    }

    /**
     * Return a concrete implementation of the abstract base class {@link ServerlessInvokerBase} for use in
     * invoking Serverless NameNode functions.
     * @param serverlessPlatformName The name of the serverless platform being used. This string should probably be
     *                               obtained from the configuration files.
     * @return Subclass of {@link ServerlessInvokerBase}.
     * @throws StorageInitializtionException
     */
    public static ServerlessInvokerBase getServerlessInvoker(String serverlessPlatformName)
            throws StorageInitializtionException {
        String invokerClassName = getInvokerClassName(serverlessPlatformName);

        if (LOG.isDebugEnabled())
            LOG.debug("Attempting to instantiate invoker of type: " + invokerClassName);

        try {
            return (ServerlessInvokerBase) Class.forName(invokerClassName).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e1) {
            throw new StorageInitializtionException(e1);
        }
    }
}
