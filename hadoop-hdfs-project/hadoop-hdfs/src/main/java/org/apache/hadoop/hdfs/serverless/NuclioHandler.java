package org.apache.hadoop.hdfs.serverless;

import com.google.gson.*;
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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

public class NuclioHandler extends BaseHandler implements EventHandler {
    //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;

    public static final Logger LOG4J_LOG = LoggerFactory.getLogger(NuclioHandler.class);

    public static io.nuclio.Logger NUCLIO_LOGGER;
    public static io.nuclio.Logger LOG = NUCLIO_LOGGER;

    public static void main(String[] args) {
        NuclioHandler handler = new NuclioHandler();

        handler.handleEvent(null,null);
    }

    static {
        System.out.println("Performing static initialization.");
        BasicConfigurator.configure();
        LOG.info("Can you see this?");
    }

    @Override
    public Response handleEvent(Context context, Event event) {
        if (NUCLIO_LOGGER == null) {
            NUCLIO_LOGGER = context.getLogger();
            LOG = NUCLIO_LOGGER;
        }

        System.out.println("Testing 321.");
        LOG4J_LOG.info("Testing 123.");

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

        String bodyAsString = new String(eventBody, StandardCharsets.UTF_8);
        JsonParser jsonParser = new JsonParser();
        JsonObject args = (JsonObject) jsonParser.parse(bodyAsString);

        if (args == null) {
            LOG.error("Could not decode event body to valid JSON object...");
            return new Response().setBody("ERROR: Failed to decode event body to valid JSON.");
        }

        return new Response().setBody("Hello, world. ");
//        JsonObject response = OpenWhiskHandler.main(args);
//
//        return new Response()
//                .setContentType("application/json")
//                .setBody(response.toString());
    }
}
