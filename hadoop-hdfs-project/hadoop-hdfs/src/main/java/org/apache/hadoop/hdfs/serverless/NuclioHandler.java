package org.apache.hadoop.hdfs.serverless;

import com.google.gson.*;
import io.nuclio.Context;
import io.nuclio.Event;
import io.nuclio.EventHandler;
import io.nuclio.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;

public class NuclioHandler extends BaseHandler implements EventHandler {
    public static final Logger LOG = LoggerFactory.getLogger(NuclioHandler.class);

    @Override
    public Response handleEvent(Context context, Event event) {
        byte[] eventBody = event.getBody();

        if (eventBody == null || eventBody.length == 0) {
            LOG.debug("Received request with empty body. Probably a HEALTHCHECK.");
            return new Response().setBody("Healthy."); // I don't think it matters what we return here.
        }

        String bodyAsString = new String(eventBody, StandardCharsets.UTF_8);
        JsonParser jsonParser = new JsonParser();
        JsonObject args = (JsonObject) jsonParser.parse(bodyAsString);
        if (args == null) {
            LOG.error("Could not decode event body to valid JSON object...");
            return new Response().setBody("ERROR: Failed to decode event body to valid JSON.");
        }

        LOG.debug("Decoded JSON payload to: " + args);
        JsonObject response = OpenWhiskHandler.main(args);
        return new Response()
                .setContentType("application/json")
                .setBody(response.toString());
    }
}
