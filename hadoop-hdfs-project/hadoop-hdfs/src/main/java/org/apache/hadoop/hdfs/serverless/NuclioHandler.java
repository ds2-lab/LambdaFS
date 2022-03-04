package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.logicalclocks.shaded.org.apache.commons.lang3.time.DurationFormatUtils;
import com.mysql.clusterj.ClusterJHelper;
import io.hops.metrics.TransactionEvent;
import io.nuclio.Context;
import io.nuclio.Event;
import io.nuclio.EventHandler;
import io.nuclio.Response;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.operation.ConsistencyProtocol;
import org.apache.hadoop.hdfs.serverless.operation.execution.NameNodeResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.serverless.OpenWhiskHandler.getLogLevelFromString;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.CONSISTENCY_PROTOCOL_ENABLED;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.LOG_LEVEL;

public class NuclioHandler implements EventHandler {
    public static final Logger LOG = LoggerFactory.getLogger(NuclioHandler.class.getName());

    /**
     * Some transactions are performed while creating the NameNode. Obviously the NameNode does not exist until
     * it is finished being created, so we store the TransactionEvent instances from those transactions here.
     * Once the NameNode is created, we add these events to the NameNode instance.
     */
    public static ThreadLocal<Set<TransactionEvent>> temporaryEventSet = new ThreadLocal<>();

    /**
     * Used internally to determine whether this instance is warm or cold.
     */
    private static boolean isCold = true;

    public static AtomicInteger activeRequestCounter = new AtomicInteger(0);

    @Override
    public Response handleEvent(Context context, Event event) {

        // Should ALWAYS be true.
        if (event != null) {
            LOG.info("Testing 123, I repeat, testing 123: INFO");
            LOG.debug("Testing 123, I repeat, testing 123: DEBUG");
            System.out.println("Testing 123, I repeat, testing 123: System.out.println");
            System.err.println("Testing 123, I repeat, testing 123: System.err.println");

            io.nuclio.Logger nuclioLogger = context.getLogger();
            nuclioLogger.info("LOG.isInfoEnabled(): " + LOG.isInfoEnabled());
            nuclioLogger.info("LOG.isDebugEnabled(): " + LOG.isDebugEnabled());

            LogManager.getRootLogger().setLevel(getLogLevelFromString("INFO"));
            nuclioLogger.info("LOG.isInfoEnabled(): " + LOG.isInfoEnabled());
            nuclioLogger.info("LOG.isDebugEnabled(): " + LOG.isDebugEnabled());

            nuclioLogger.info("LOG.isWarnEnabled(): " + LOG.isWarnEnabled());
            nuclioLogger.info("LOG.isErrorEnabled(): " + LOG.isErrorEnabled());
            nuclioLogger.info("LOG.isTraceEnabled(): " + LOG.isTraceEnabled());
            nuclioLogger.info("NuclioLogger -- testing123: INFO");
            nuclioLogger.debug("NuclioLogger -- testing123: DEBUG");
            nuclioLogger.warn("NuclioLogger -- testing123: WARN");
            nuclioLogger.error("NuclioLogger -- testing123: ERROR");

            return new Response().setBody("Hello, world! v2 ");
        }

        long startTime = System.nanoTime();
        String functionName = platformSpecificInitialization(event);

        LOG.info("============================================================");
        LOG.info(functionName + " v" + ServerlessNameNode.versionNumber + " received HTTP request.");
        int activeRequests = activeRequestCounter.incrementAndGet();
        LOG.info("Active HTTP requests: " + activeRequests);
        LOG.info("============================================================\n");

        byte[] eventBody = event.getBody();
        JsonObject args = null;
        ObjectInput in = null;
        ByteArrayInputStream bis = new ByteArrayInputStream(eventBody);
        try {
            in = new ObjectInputStream(bis);
        } catch (IOException e) {
            LOG.error("Failed to create ObjectOutputStream for Event body.");
            e.printStackTrace();

            JsonObject response = new JsonObject();

            // TODO: Return an error to user/client here.
        }

        try {
            args = (JsonObject)in.readObject();
        } catch (ClassNotFoundException | IOException e) {
            LOG.error("Failed to read in object from Event body.");
            e.printStackTrace();

            // TODO: Return an error to user/client here.
        }

        assert args != null;
        JsonObject response = OpenWhiskHandler.main(args);

        return new Response()
                .setContentType("application/json")
                .setBody(response.toString());
    }

    /**
     * In this case, we are performing OpenWhisk-specific initialization.
     *
     * @return The name of this particular OpenWhisk serverless function/action. Note that the namespace portion
     * of the function's name is removed. So, if the function's fully-qualified name is "/whisk.system/namenode0",
     * then we return "namenode0", removing the "/whisk.system/" from the function's name.
     */
    private static String platformSpecificInitialization(Event event) {
        String functionNameWithNamespace = event.getPath();
        Path functionNameAsPath = Paths.get(functionNameWithNamespace);

        // This will extract just the last part. The function names are really:
        // /whisk.system/namenodeX, where X is an integer. We don't want the "/whisk.system/" part.
        return functionNameAsPath.getFileName().toString();
    }
}
