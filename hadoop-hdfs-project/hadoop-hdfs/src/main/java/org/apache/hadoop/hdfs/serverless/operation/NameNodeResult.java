package org.apache.hadoop.hdfs.serverless.operation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

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
     * Flag which indicates whether there is a result.
     */
    private boolean hasResult = false;

    public NameNodeResult() {
        exceptions = new ArrayList<Throwable>();
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
        } else {
            LOG.info("Number of exceptions: 0");
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
     * @param operation If non-null, will be included as a top-level entry in the result under the key "op".
     *                  This is used by TCP connections in particular, as TCP messages generally contain an "op"
     *                  field to designate the request as one containing a result or one used for registration.
     */
    public JsonObject toJson(String operation) {
        JsonObject json = new JsonObject();

        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            ObjectOutputStream objectOutputStream;
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(result);
            objectOutputStream.flush();

            byte[] objectBytes = byteArrayOutputStream.toByteArray();
            String base64Object = Base64.encodeBase64String(objectBytes);

            json.addProperty("RESULT", base64Object);
        } catch (Exception ex) {
            LOG.error("Exception encountered whilst serializing result of file system operation: ", ex);
            addException(ex);
        }

        if (exceptions.size() > 0) {
            JsonArray exceptionsJson = new JsonArray();

            for (Throwable t : exceptions) {
                exceptionsJson.add(t.toString());
            }

            json.add("EXCEPTIONS", exceptionsJson);
        }

        if (serverlessFunctionMapping != null) {
            // Embed all the information about the serverless function mapping in the Json response.
            JsonObject functionMapping = new JsonObject();
            functionMapping.addProperty("fileOrDirectory", serverlessFunctionMapping.fileOrDirectory);
            functionMapping.addProperty("parentId", serverlessFunctionMapping.parentId);
            functionMapping.addProperty("function", serverlessFunctionMapping.mappedFunctionNumber);

            json.add("FUNCTION_MAPPING", functionMapping);
        }

        if (operation != null)
            json.add("op", operation);

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
