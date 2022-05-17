package org.apache.hadoop.hdfs.serverless;

import io.hops.metrics.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class BaseHandler {
    // public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(BaseHandler.class);

    /**
     * Means we're running within a Docker container on some VM (and not in an OpenWhisk cluster).
     */
    public static volatile boolean localModeEnabled = false;

    /**
     * Defines the platform we're using. This will eventually just be controlled by a configuration parameter.
     *
     * TODO: Make this so it is exclusively set by the config. The reason it isn't now is that the config is
     *       normally parsed AFTER the NameNode is created, but there is some platform-specific code that runs
     *       BEFORE the NameNode is created. The solution to this is to just rework things a bit so that platform-
     *       specific code doesn't run until after the NameNode is created. This is not actually a difficult change
     *       to implement; it just hasn't been high-priority...
     */
    public static final String PLATFORM_NAME = "openwhisk";

    /**
     * Some transactions are performed while creating the NameNode. Obviously the NameNode does not exist until
     * it is finished being created, so we store the TransactionEvent instances from those transactions here.
     * Once the NameNode is created, we add these events to the NameNode instance.
     */
    public static ThreadLocal<Set<TransactionEvent>> temporaryEventSet = new ThreadLocal<>();

    public static ThreadLocal<String> currentRequestId = new ThreadLocal<>();

    /**
     * Perform initialization specific to the particular platform.
     *
     * @return The name of the serverless function.
     */
    protected static String platformSpecificInitialization() {
        if (PLATFORM_NAME.equalsIgnoreCase("openwhisk"))
            return openWhiskInitialization();
        else if (PLATFORM_NAME.equalsIgnoreCase("nuclio"))
            return nuclioInitialization();
        else
            throw new IllegalStateException("Unsupported serverless platform specified.");
    }

    private static String nuclioInitialization() {
        LOG.info("Performing platform-specific initialization...");
        LOG.debug("Hadoop configuration directory: " + System.getenv("HADOOP_CONF_DIR"));
        String nuclioFunctionName = System.getenv("NUCLIO_FUNCTION_NAME");
        LOG.info("Nuclio function name: " + nuclioFunctionName);
        return nuclioFunctionName;
    }

    /**
     * @return The name of this particular OpenWhisk serverless function/action. Note that the namespace portion
     * of the function's name is removed. So, if the function's fully-qualified name is "/whisk.system/namenode0",
     * then we return "namenode0", removing the "/whisk.system/" from the function's name.
     */
    private static String openWhiskInitialization() {
        String activationId = System.getenv("__OW_ACTIVATION_ID");

        LOG.debug("Hadoop configuration directory: " + System.getenv("HADOOP_CONF_DIR"));
        LOG.debug("OpenWhisk activation ID: " + activationId);

        String functionNameWithNamespace = System.getenv("__OW_ACTION_NAME");
        Path functionNameAsPath = Paths.get(functionNameWithNamespace);

        // This will extract just the last part. The function names are really:
        // /whisk.system/namenodeX, where X is an integer. We don't want the "/whisk.system/" part.
        return functionNameAsPath.getFileName().toString();
    }
}
