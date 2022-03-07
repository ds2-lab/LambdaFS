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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.serverless.OpenWhiskHandler.getLogLevelFromString;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.CONSISTENCY_PROTOCOL_ENABLED;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.LOG_LEVEL;

public class NuclioHandler implements EventHandler {
    //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;

    public static io.nuclio.Logger NUCLIO_LOGGER;
    public static io.nuclio.Logger LOG = NUCLIO_LOGGER;

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

    public static void main(String[] args) {
        NuclioHandler handler = new NuclioHandler();

        handler.handleEvent(null,null);
    }

    @Override
    public Response handleEvent(Context context, Event event) {
        if (NUCLIO_LOGGER == null)
            NUCLIO_LOGGER = context.getLogger();

        NUCLIO_LOGGER.info("Event Header: " + event.getHeaders().toString());
        NUCLIO_LOGGER.info("Event Body: " + Arrays.toString(event.getBody()));

        byte[] eventBody = event.getBody();

        if (eventBody == null || eventBody.length == 0) {
            NUCLIO_LOGGER.info("Received request with empty body. Probably a HEALTHCHECK.");
            return new Response().setBody("Healthy."); // I don't think it matters what we return here.
        } else {
            NUCLIO_LOGGER.info("Request body: " + event);
        }

        long startTime = System.nanoTime();
        String functionName = platformSpecificInitialization(event);

        LOG.info("============================================================");
        LOG.info(functionName + " v" + ServerlessNameNode.versionNumber + " received HTTP request.");
        int activeRequests = activeRequestCounter.incrementAndGet();
        LOG.info("Active HTTP requests: " + activeRequests);
        LOG.info("============================================================\n");

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
