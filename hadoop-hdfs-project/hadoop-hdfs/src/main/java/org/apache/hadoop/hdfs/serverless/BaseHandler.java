package org.apache.hadoop.hdfs.serverless;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseHandler {
    public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;

    /**
     * Defines the platform we're using. This will eventually just be controlled by a configuration parameter.
     */
    public static final String PLATFORM_NAME = "nuclio";

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
