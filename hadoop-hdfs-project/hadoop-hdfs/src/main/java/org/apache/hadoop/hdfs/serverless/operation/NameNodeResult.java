package org.apache.hadoop.hdfs.serverless.operation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This encapsulates all the information that may be returned to the user after the NameNode executes.
 *
 * This includes final results from FS operations, exceptions, INode mapping information, etc.
 *
 * As the NameNode executes, it adds any exceptions it encounters to this class' list. When it comes time to
 * return to the client, this class dumps all of its data into a JsonObject that the client will know how to process.
 *
 * This is used on the NameNode side.
 */
public class NameNodeResult {
    private static final Logger LOG = LoggerFactory.getLogger(NameNodeResult.class);

    /**
     * Exceptions encountered during the current request's execution.
     */
    private final ArrayList<Throwable> exceptions;

    /**
     * The desired result of the current request's execution.
     */
    private Object result;

    /**
     * We may be returning a mapping of a file or directory to a particular serverless function.
     */
    private ServerlessFunctionMapping serverlessFunctionMapping;

    /**
     * The name of the serverless function all of this is running in/on.
     */
    private final String functionName;

    /**
     * Flag which indicates whether there is a result.
     */
    private boolean hasResult = false;

    /**
     * Any extra fields to be added explicitly/directly to the result payload. As of right now,
     * only strings are supported as additional fields.
     *
     * These will be added as a top-level key/value pair to the JSON payload returned to the client.
     */
    private final HashMap<String, String> additionalFields;

    /**
     * Request ID associated with this result.
     */
    private final String requestId;


    public NameNodeResult(String functionName, String requestId) {
        this.functionName = functionName;
        this.requestId = requestId;
        this.exceptions = new ArrayList<>();
        this.additionalFields = new HashMap<>();
    }

    /**
     * Update the result field of this request's execution.
     * @param result The result of executing the desired operation.
     * @param forceOverwrite If true, overwrite an existing result.
     * @return True if the result was stored, otherwise false.
     */
    public boolean addResult(Object result, boolean forceOverwrite) {
        if (result == null || forceOverwrite) {
            this.result = result;
            hasResult = true;
            return true;
        } else {
            LOG.warn("Cannot overwrite existing result of type " + result.getClass().getSimpleName() + ".");
        }

        return false;
    }

    /**
     * Store the file/directory-to-serverless-function mapping information so that it may be returned to
     * whoever invoked us.
     *
     * @param fileOrDirectory The file or directory being mapped to a function.
     * @param parentId The ID of the file or directory's parent iNode.
     * @param mappedFunctionName The name of the serverless function to which the file or directory was mapped.
     */
    public void addFunctionMapping(String fileOrDirectory, long parentId, int mappedFunctionName) {
        this.serverlessFunctionMapping = new ServerlessFunctionMapping(fileOrDirectory, parentId, mappedFunctionName);
    }

    /**
     * Explicitly add an entry as a top-level key/value pair to the payload returned to the client.
     *
     * This should only be used for entries that are not covered by the rest of the API.
     * @param key The key to use for the entry.
     * @param value The value to be used for the entry.
     */
    public void addExtraString(String key, String value) {
        this.additionalFields.put(key, value);
    }

    /**
     * Formally record/take note that an exception occurred.
     * @param ex The exception that occurred.
     */
    public void addException(Exception ex) {
        exceptions.add(ex);
    }

    /**
     * Returns true if there's a result, otherwise false.
     */
    public boolean getHasResult() {
        return hasResult;
    }

    /**
     * Alias for `addException()`.
     * @param t The exception/throwable to record.
     */
    public void addThrowable(Throwable t) {
        exceptions.add(t);
    }

    /**
     * Log some useful debug information about the result field (e.g., the object's type) to the console.
     */
    public void logResultDebugInformation() {
        LOG.info("+-+-+-+-+-+-+ Result Debug Information +-+-+-+-+-+-+");
        if (hasResult) {
            LOG.info("Type: " + result.getClass());
            LOG.info("Result " + (result instanceof Serializable ? "IS " : "is NOT ") + "serializable.");
            LOG.info("Value: " + result);
        } else {
            LOG.info("There is NO result value present.");
        }

        LOG.info("----------------------------------------------------");

        LOG.info("Number of exceptions: " + exceptions.size());

        if (exceptions.size() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("Exceptions:\n");

            int counter = 1;
            for (Throwable ex : exceptions) {
                builder.append(counter);
                builder.append(": ");
                builder.append(ex.getClass().getSimpleName());
                builder.append(": ");
                builder.append(ex.getCause());
                builder.append("\n");
                counter++;
            }

            LOG.info(builder.toString());
        }

        LOG.info("----------------------------------------------------");

        if (serverlessFunctionMapping != null) {
            LOG.info(serverlessFunctionMapping.toString());
        } else {
            LOG.info("No serverless function mapping data contained within this result.");
        }

        LOG.info("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+");
    }

    /**
     * Convert this object to a JsonObject so that it can be returned directly to the invoker.
     *
     * This inspects the type of the result field. If it is of type DuplicateRequest, an additional field
     * is added to the payload indicating that this response is for a duplicate request and should not be
     * returned as the true result of the associated file system operation.
     *
     * @param operation If non-null, will be included as a top-level entry in the result under the key "op".
     *                  This is used by TCP connections in particular, as TCP messages generally contain an "op"
     *                  field to designate the request as one containing a result or one used for registration.
     */
    public JsonObject toJson(String operation) {
        JsonObject json = new JsonObject();

        // If the result is a duplicate request, then don't bother sending an actual result field.
        // That's just unnecessary network I/O. We can just include a flag indicating that this is
        // a duplicate request and leave it at that.
        if (result instanceof DuplicateRequest) {
            // Add a flag indicating whether this is just a duplicate result.
            json.addProperty(ServerlessNameNodeKeys.DUPLICATE_REQUEST, true);
        } else {
            LOG.debug("Result type: " + (result != null ? result.getClass().getSimpleName() : "null"));

            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                ObjectOutputStream objectOutputStream;
                objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                objectOutputStream.writeObject(result);
                objectOutputStream.flush();

                byte[] objectBytes = byteArrayOutputStream.toByteArray();
                String base64Object = Base64.encodeBase64String(objectBytes);

                json.addProperty(ServerlessNameNodeKeys.RESULT, base64Object);
                json.addProperty(ServerlessNameNodeKeys.DUPLICATE_REQUEST, false);
            } catch (Exception ex) {
                LOG.error("Exception encountered whilst serializing result of file system operation: ", ex);
                addException(ex);
            }
        }

        if (exceptions.size() > 0) {
            JsonArray exceptionsJson = new JsonArray();

            for (Throwable t : exceptions) {
                exceptionsJson.add(t.toString());
            }

            json.add(ServerlessNameNodeKeys.EXCEPTIONS, exceptionsJson);
        }

        if (serverlessFunctionMapping != null) {
            // Embed all the information about the serverless function mapping in the Json response.
            JsonObject functionMapping = new JsonObject();
            functionMapping.addProperty("fileOrDirectory", serverlessFunctionMapping.fileOrDirectory);
            functionMapping.addProperty("parentId", serverlessFunctionMapping.parentId);
            functionMapping.addProperty("function", serverlessFunctionMapping.mappedFunctionNumber);

            json.add("FUNCTION_MAPPING", functionMapping);
        }

        if (additionalFields.size() > 0) {
            for (Map.Entry<String, String> entry : additionalFields.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                json.addProperty(key, value);
            }
        }

        if (operation != null)
            json.addProperty(ServerlessNameNodeKeys.OPERATION, operation);

        json.addProperty(ServerlessNameNodeKeys.FUNCTION_NAME, functionName);

        json.addProperty(ServerlessNameNodeKeys.REQUEST_ID, requestId);

        return json;
    }

    /**
     * Return the number of exceptions contained within this result.
     */
    public int numExceptions() {
        return exceptions.size();
    }

    /**
     * Encapsulates the mapping of a particular file or directory to a particular serverless function.
     */
    static class ServerlessFunctionMapping {
        /**
         * The file or directory that we're mapping to a serverless function.
         */
        String fileOrDirectory;

        /**
         * The ID of the file or directory's parent iNode.
         */
        long parentId;

        /**
         * The number of the serverless function to which the file or directory was mapped.
         */
        int mappedFunctionNumber;

        public ServerlessFunctionMapping(String fileOrDirectory, long parentId, int mappedFunctionNumber) {
            this.fileOrDirectory = fileOrDirectory;
            this.parentId = parentId;
            this.mappedFunctionNumber = mappedFunctionNumber;
        }

        @Override
        public String toString() {
            return "FunctionMapping: [src=" + fileOrDirectory + ", parentId=" + parentId
                    + ", targetFunctionNumber=" + mappedFunctionNumber + "]";
        }
    }
}
