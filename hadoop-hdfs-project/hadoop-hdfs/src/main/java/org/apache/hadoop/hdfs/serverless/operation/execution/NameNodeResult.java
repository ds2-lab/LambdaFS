package org.apache.hadoop.hdfs.serverless.operation.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import org.apache.hadoop.hdfs.serverless.cache.InMemoryINodeCache;
import org.apache.hadoop.hdfs.serverless.cache.MetadataCacheManager;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCacheManager;
import org.apache.hadoop.hdfs.serverless.operation.ActiveServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.operation.ActiveServerlessNameNodeList;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.nustaq.serialization.FSTConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;

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
public final class NameNodeResult implements Serializable {
    //private static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(NameNodeResult.class);
    private static final long serialVersionUID = -6018521672360252605L;

    /**
     * From RuedigerMoeller/fast-serialization.
     */
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    /**
     * One easy and important optimization is to register classes which are serialized for sure in your application
     * at the FSTConfiguration object. This way FST can avoid writing classnames.
     */
    static {
        conf.registerClass(LocatedBlocks.class, TransactionEvent.class, TransactionAttempt.class, NamespaceInfo.class,
                LastBlockWithStatus.class, HdfsFileStatus.class, DirectoryListing.class, FsServerDefaults.class,
                ActiveServerlessNameNodeList.class, ActiveServerlessNameNode.class);
    }

    /**
     * Exceptions encountered during the current request's execution.
     */
    private ArrayList<Throwable> exceptions;

    /**
     * The desired result of the current request's execution.
     */
    private Serializable result;

    /**
     * These are committed to the result object right after its associated operation has been completed.
     */
    private String statisticsPackageSerializedAndEncoded;

    /**
     * Transaction events that have been serialized and encoded.
     * Only used for HTTP requests. TCP requests use the {@code txEvents} variable.
     */
    private String txEventsSerializedAndEncoded;

    /**
     * TX events. A TX Event encapsulates metric information about a particular transaction.
     * Only used for TCP requests. HTTP requests use the {@code txEventsSerializedAndEncoded} variable.
     */
    private List<TransactionEvent> txEvents;

    /**
     * We may be returning a mapping of a file or directory to a particular serverless function.
     */
    private ServerlessFunctionMapping serverlessFunctionMapping;

    /**
     * The name of the serverless function all of this is running in/on.
     */
    private int deploymentNumber;

    /**
     * The unique ID of the current NameNode instance.
     */
    private long nameNodeId;

    /**
     * Flag which indicates whether there is a result.
     */
    private boolean hasResult = false;

    /**
     * Indicates whether the NameNode was cold-started when computing this result.
     * Only used for HTTP, as TCP cannot be used except with warm NameNodes.
     */
    private boolean coldStart = false;

    /**
     * Request ID associated with this result.
     */
    private String requestId;

    /**
     * HTTP or TCP.
     */
    private String requestMethod;

    /**
     * Time at which the serverless function received the request associated with this result.
     */
    private long fnStartTime;

    /**
     * Roughly the time at which the FN exited. This is set in the {@code prepare} method, which is supposed to be
     * called as the last step before sending this result back to the client (either via TCP or HTTP).
     */
    private long fnEndTime;

    /**
     * Time at which this request was enqueued in the NameNode's work queue.
     */
    private long enqueuedTime;

    /**
     * Time at which the NameNode dequeued this request from the work queue and began
     * executing it.
     */
    private long dequeuedTime;

    /**
     * Time at which the execution/in-memory processing of the operation finishes. The remaining time would be any
     * post-processing, packaging the result up for the client, and transporting it back to the client.
     */
    private long processingFinishedTime;

    /**
     * Name of the FS operation we performed.
     */
    private String operationName;

    /**
     * Indicates whether this result corresponds to a duplicate request.
     */
    private boolean isDuplicate = false;

    /**
     * Number of metadata cache hits that occurred while executing the corresponding FS operation.
     */
    private int metadataCacheHits = 0;

    /**
     * Number of metadata cache misses that occurred while executing the corresponding FS operation.
     */
    private int metadataCacheMisses = 0;

    /**
     * The UTC timestamp at which this result was (theoretically) delivered back to the client.
     *
     * A value of -1 indicates that this result has not yet been delivered. A non-negative value
     * does not indicate that the result was delivered successfully, only that the NameNode
     * attempted to deliver it.
     */
    private long timeDeliveredBackToClient = -1L;

    public NameNodeResult(int deploymentNumber, String requestId, String requestMethod, long nameNodeId, String operationName) {
        this.deploymentNumber = deploymentNumber;
        this.nameNodeId = nameNodeId;
        this.requestId = requestId;
        this.exceptions = new ArrayList<>();
        this.requestMethod = requestMethod;
        this.operationName = operationName;
    }

    // Empty constructor used for Kryo serialization.
    private NameNodeResult() {

    }

    /**
     * Update the result field of this request's execution.
     * @param result The result of executing the desired operation.
     * @param forceOverwrite If true, overwrite an existing result.
     * @return True if the result was stored, otherwise false.
     */
    public boolean addResult(Serializable result, boolean forceOverwrite) {
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

    public void setColdStart(boolean coldStart) {
        this.coldStart = coldStart;
    }

    /**
     * Formally record/take note that an exception occurred.
     * @param ex The exception that occurred.
     */
    public void addException(Exception ex) {
        addThrowable(ex);
    }

    /**
     * Returns true if there's a result, otherwise false.
     */
    public boolean getHasResult() {
        return hasResult;
    }

    /**
     * Essentially an alias for `addException()`, though there are Throwables that are not exceptions (e.g.,
     * IllegalAccessError) that we could encounter.
     *
     * @param t The exception/throwable to record.
     */
    public void addThrowable(Throwable t) {
        exceptions.add(t);
    }

    /**
     * Log some useful debug information about the result field (e.g., the object's type) to the console.
     */
    public void logResultDebugInformation(String opPerformed) {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug("+-+-+-+-+-+-+ Result Debug Information +-+-+-+-+-+-+");
        if (hasResult) {
            LOG.debug("Type: " + result.getClass());
            LOG.debug("Result " + (result instanceof Serializable ? "IS " : "is NOT ") + "serializable.");
            LOG.debug("Value: " + result);
        } else {
            LOG.debug("There is NO result value present.");
        }

        if (opPerformed != null)
            LOG.debug("Operation performed: " + opPerformed);

        LOG.debug("----------------------------------------------------");

        LOG.debug("NameNode ID: " + this.nameNodeId);
        LOG.debug("Deployment number: " + this.deploymentNumber);
        LOG.debug("Number of exceptions: " + exceptions.size());

        if (exceptions.size() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("Exceptions:\n");

            int counter = 1;
            for (Throwable ex : exceptions) {
                builder.append("Exception #");
                builder.append(counter);
                builder.append(": ");
                builder.append(ex.getClass().getSimpleName());
                builder.append(": ");
                builder.append(ex.getMessage());
                builder.append("\n");
                counter++;
            }

            LOG.debug(builder.toString());
        }

        LOG.debug("----------------------------------------------------");

        if (serverlessFunctionMapping != null) {
            LOG.debug(serverlessFunctionMapping.toString());
        } else {
            LOG.debug("No serverless function mapping data contained within this result.");
        }

        LOG.debug("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+\n");
    }

    public String serializeAndEncode(Object object) {
//        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
//            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//            objectOutputStream.writeObject(object);
//            objectOutputStream.flush();
//
//            byte[] objectBytes = byteArrayOutputStream.toByteArray();
//
//            return Base64.encodeBase64String(objectBytes);
//        } catch (Exception ex) {
//            LOG.error("Exception encountered whilst serializing result of file system operation: ", ex);
//            addException(ex);
//        }

        byte[] objectBytes = conf.asByteArray(object);
        return Base64.encodeBase64String(objectBytes);
    }

    /**
     * Convert this object to a JsonObject so that it can be returned directly to the invoker.
     *
     * This inspects the type of the result field. If it is of type DuplicateRequest, an additional field
     * is added to the payload indicating that this response is for a duplicate request and should not be
     * returned as the true result of the associated file system operation.
     *
     * @param metadataCacheManager The manager of the in-memory metadata caches so we can get cache hit/miss metrics.
     *
     *                      We do NOT clear the counters on the metadataCache object after extracting them.
     * @param operation If non-null, will be included as a top-level entry in the result under the key "op".
     *                  This is used by TCP connections in particular, as TCP messages generally contain an "op"
     *                  field to designate the request as one containing a result or one used for registration.
     */
    public ObjectNode toJsonJackson(String operation, MetadataCacheManager metadataCacheManager) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();

        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        // If the result is a duplicate request, then don't bother sending an actual result field.
        // That's just unnecessary network I/O. We can just include a flag indicating that this is
        // a duplicate request and leave it at that.
        if (result instanceof DuplicateRequest) {
            // Add a flag indicating whether this is just a duplicate result.
            json.put(ServerlessNameNodeKeys.DUPLICATE_REQUEST, true);
        } else {
            if (result != null && LOG.isDebugEnabled())
                LOG.debug("Returning result of type " + result.getClass().getSimpleName()
                        + " to client for request " + requestId + ".");

            String resultSerializedAndEncoded = serializeAndEncode(result);

            if (resultSerializedAndEncoded != null)
                json.put(ServerlessNameNodeKeys.RESULT, resultSerializedAndEncoded);
            json.put(ServerlessNameNodeKeys.DUPLICATE_REQUEST, false);
        }

        if (exceptions.size() > 0) {
            ArrayNode exceptionsJson = mapper.createArrayNode();

            for (Throwable t : exceptions) {
                exceptionsJson.add(t.toString());
            }

            json.set(ServerlessNameNodeKeys.EXCEPTIONS, exceptionsJson);
        }

        if (serverlessFunctionMapping != null) {
            // Embed all the information about the serverless function mapping in the Json response.
            ObjectNode functionMapping = mapper.createObjectNode();
            functionMapping.put(FILE_OR_DIR, serverlessFunctionMapping.fileOrDirectory);
            functionMapping.put(PARENT_ID, serverlessFunctionMapping.parentId);
            functionMapping.put(FUNCTION, serverlessFunctionMapping.mappedFunctionNumber);

            json.set(DEPLOYMENT_MAPPING, functionMapping);
        }

        if (operation != null)
            json.put(ServerlessNameNodeKeys.OPERATION, operation);

        json.put(ServerlessNameNodeKeys.NAME_NODE_ID, nameNodeId);
        json.put(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER, deploymentNumber);
        json.put(ServerlessNameNodeKeys.REQUEST_ID, requestId);
        json.put(ServerlessNameNodeKeys.REQUEST_METHOD, requestMethod);
        json.put(ServerlessNameNodeKeys.CANCELLED, false);
        json.put(ServerlessNameNodeKeys.OPENWHISK_ACTIVATION_ID, "N/A"); //System.getenv("__OW_ACTIVATION_ID"));
        int totalCacheHits = metadataCache.getNumCacheHitsCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheHits();
        int totalCacheMisses = metadataCache.getNumCacheMissesCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheMisses();
        json.put(ServerlessNameNodeKeys.CACHE_HITS, totalCacheHits);
        json.put(ServerlessNameNodeKeys.CACHE_MISSES, totalCacheMisses);
        json.put(ServerlessNameNodeKeys.FN_START_TIME, fnStartTime);
        json.put(ServerlessNameNodeKeys.ENQUEUED_TIME, enqueuedTime);
        json.put(ServerlessNameNodeKeys.DEQUEUED_TIME, dequeuedTime);
        json.put(ServerlessNameNodeKeys.PROCESSING_FINISHED_TIME, processingFinishedTime);
        json.put(COLD_START, coldStart);

        if (statisticsPackageSerializedAndEncoded != null)
            json.put(ServerlessNameNodeKeys.STATISTICS_PACKAGE, statisticsPackageSerializedAndEncoded);

        if (txEventsSerializedAndEncoded != null)
            json.put(ServerlessNameNodeKeys.TRANSACTION_EVENTS, txEventsSerializedAndEncoded);

        // Reset these in-case this thread gets re-used in the future for another request.
        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        json.put(ServerlessNameNodeKeys.FN_END_TIME, System.currentTimeMillis());
        return json;
    }

    /**
     * Called before sending the result via TCP.
     */
    public void prepare(MetadataCacheManager metadataCacheManager) {
        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        if (result instanceof DuplicateRequest)
            isDuplicate = true;

        metadataCacheHits = metadataCache.getNumCacheHitsCurrentRequest();      // + replicaCacheManager.getThreadLocalCacheHits();
        metadataCacheMisses = metadataCache.getNumCacheMissesCurrentRequest();  // + replicaCacheManager.getThreadLocalCacheMisses();

        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        fnEndTime = System.currentTimeMillis();
    }

    public List<TransactionEvent> getTxEvents() { return this.txEvents; }

    /**
     * Convert this object to a JsonObject so that it can be returned directly to the invoker.
     *
     * This inspects the type of the result field. If it is of type DuplicateRequest, an additional field
     * is added to the payload indicating that this response is for a duplicate request and should not be
     * returned as the true result of the associated file system operation.
     *
     * @param metadataCacheManager The manager of the in-memory metadata caches so we can get cache hit/miss metrics.
     *
     *                      We do NOT clear the counters on the metadataCache object after extracting them.
     * @param operation If non-null, will be included as a top-level entry in the result under the key "op".
     *                  This is used by TCP connections in particular, as TCP messages generally contain an "op"
     *                  field to designate the request as one containing a result or one used for registration.
     */
    public JsonObject toJson(String operation, MetadataCacheManager metadataCacheManager) {
        JsonObject json = new JsonObject();
        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        // If the result is a duplicate request, then don't bother sending an actual result field.
        // That's just unnecessary network I/O. We can just include a flag indicating that this is
        // a duplicate request and leave it at that.
        if (result instanceof DuplicateRequest) {
            // Add a flag indicating whether this is just a duplicate result.
            json.addProperty(ServerlessNameNodeKeys.DUPLICATE_REQUEST, true);
        } else {
            if (result != null && LOG.isDebugEnabled())
                LOG.debug("Returning result of type " + result.getClass().getSimpleName()
                        + " to client. Result value: " + result.toString());

            String resultSerializedAndEncoded = serializeAndEncode(result);

            if (resultSerializedAndEncoded != null)
                json.addProperty(ServerlessNameNodeKeys.RESULT, resultSerializedAndEncoded);
            json.addProperty(ServerlessNameNodeKeys.DUPLICATE_REQUEST, false);
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
            functionMapping.addProperty(FILE_OR_DIR, serverlessFunctionMapping.fileOrDirectory);
            functionMapping.addProperty(PARENT_ID, serverlessFunctionMapping.parentId);
            functionMapping.addProperty(FUNCTION, serverlessFunctionMapping.mappedFunctionNumber);

            json.add(DEPLOYMENT_MAPPING, functionMapping);
        }

        if (operation != null)
            json.addProperty(ServerlessNameNodeKeys.OPERATION, operation);

        json.addProperty(ServerlessNameNodeKeys.NAME_NODE_ID, nameNodeId);
        json.addProperty(ServerlessNameNodeKeys.DEPLOYMENT_NUMBER, deploymentNumber);
        json.addProperty(ServerlessNameNodeKeys.REQUEST_ID, requestId);
        json.addProperty(ServerlessNameNodeKeys.REQUEST_METHOD, requestMethod);
        json.addProperty(ServerlessNameNodeKeys.CANCELLED, false);
        json.addProperty(ServerlessNameNodeKeys.OPENWHISK_ACTIVATION_ID, System.getenv("__OW_ACTIVATION_ID"));
        int totalCacheHits = metadataCache.getNumCacheHitsCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheHits();
        int totalCacheMisses = metadataCache.getNumCacheMissesCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheMisses();
        json.addProperty(ServerlessNameNodeKeys.CACHE_HITS, totalCacheHits);
        json.addProperty(ServerlessNameNodeKeys.CACHE_MISSES, totalCacheMisses);
        json.addProperty(ServerlessNameNodeKeys.FN_START_TIME, fnStartTime);
        json.addProperty(ServerlessNameNodeKeys.ENQUEUED_TIME, enqueuedTime);
        json.addProperty(ServerlessNameNodeKeys.DEQUEUED_TIME, dequeuedTime);
        json.addProperty(ServerlessNameNodeKeys.PROCESSING_FINISHED_TIME, processingFinishedTime);
        json.addProperty(COLD_START, coldStart);

        if (statisticsPackageSerializedAndEncoded != null)
            json.addProperty(ServerlessNameNodeKeys.STATISTICS_PACKAGE, statisticsPackageSerializedAndEncoded);

        if (txEventsSerializedAndEncoded != null)
            json.addProperty(ServerlessNameNodeKeys.TRANSACTION_EVENTS, txEventsSerializedAndEncoded);

        // Reset these in-case this thread gets re-used in the future for another request.
        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        json.addProperty(ServerlessNameNodeKeys.FN_END_TIME, System.currentTimeMillis());
        return json;
    }

    /**
     * Serialize and encode the current statistics contained in the {@link TransactionsStats} instance to this
     * instance of NameNodeResult.
     */
    public void commitStatisticsPackages() {
        TransactionsStats.ServerlessStatisticsPackage statisticsPackage
                = TransactionsStats.getInstance().exportForServerless(requestId);
        if (statisticsPackage != null) {
            this.statisticsPackageSerializedAndEncoded = serializeAndEncode(statisticsPackage);
        }
    }

    public ArrayList<Throwable> getExceptions() {
        return exceptions;
    }

    public Serializable getResult() {
        return result;
    }

    public ServerlessFunctionMapping getServerlessFunctionMapping() {
        return serverlessFunctionMapping;
    }

    public int getDeploymentNumber() {
        return deploymentNumber;
    }

    public long getNameNodeId() {
        return nameNodeId;
    }

    public boolean isHasResult() {
        return hasResult;
    }

    public boolean isColdStart() {
        return coldStart;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getOperationName() {
        return operationName;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void commitTransactionEvents(List<TransactionEvent> transactionEvents) {
        if (transactionEvents != null) {
            if (this.requestMethod.equals("HTTP"))
                this.txEventsSerializedAndEncoded = serializeAndEncode(transactionEvents);
            else
                this.txEvents = transactionEvents;
        }
    }

    @Override
    public String toString() {
        return "NameNodeResult(RequestID=" + requestId + ", OperationName=" + operationName + ")";
    }

    /**
     * Mark this result as being delivered back to the client. This does not indicate that the delivery was successful
     * (i.e., that the client actually received the result). Instead, it just means that the NN attempted to deliver it.
     *
     * If the timestamp is already greater than zero, this will raise an exception, UNLESS the 'redo' flag is true.
     *
     * @param redo Indicates that this result is being re-sent back to the client, and thus an existing, positive
     *             timestamp value should not result in an error.
     *
     * @throws IllegalStateException If 'redo' is false and the timestamp is already greater than zero.
     */
    public void markDelivered(boolean redo) throws IllegalStateException {
        // If the time-stamp is nonzero and the result isn't being re-delivered back to the client, then
        // throw an exception to indicate that something is wrong here.
        if (timeDeliveredBackToClient > 0 && !redo) {
            throw new IllegalStateException("This result was already delivered to the client at " +
                    Instant.ofEpochMilli(timeDeliveredBackToClient).atZone(ZoneOffset.UTC) + ".");
        }

        long newTimestamp = System.currentTimeMillis();
        String oldTimeFormatted = Instant.ofEpochMilli(timeDeliveredBackToClient).atZone(ZoneOffset.UTC).toString();
        String newTimeFormatted = Instant.ofEpochMilli(newTimestamp).atZone(ZoneOffset.UTC).toString();

        if (timeDeliveredBackToClient > 0)
            LOG.warn("Overwriting previous delivery timestamp of " + timeDeliveredBackToClient + "(" +
                    oldTimeFormatted + ") with new timestamp of " + newTimestamp + "(" + newTimeFormatted + ").");

        timeDeliveredBackToClient = newTimestamp;
    }

    /**
     * Update the 'nameNodeId' instance variable. There are cases where we create an instance of this class
     * before having access to the NameNode's ID, so we have to update the ID after instantiating this object in
     * those cases.
     *
     * @param nameNodeId The NameNode's ID.
     */
    public void setNameNodeId(long nameNodeId) {
        this.nameNodeId = nameNodeId;
    }

    /**
     * Return the number of exceptions contained within this result.
     */
    public int numExceptions() {
        return exceptions.size();
    }

    /**
     * Return the time at which the NN attempted to deliver this result back to the client.
     *
     * A value of -1 indicates that the NN has not yet attempted to deliver the result back to the client.
     */
    public long getTimeDeliveredBackToClient() {
        return timeDeliveredBackToClient;
    }

    public long getFnStartTime() {
        return fnStartTime;
    }

    public long getFnEndTime() {
        return fnEndTime;
    }

    public void setFnStartTime(long fnStartTime) {
        this.fnStartTime = fnStartTime;
    }

    public long getEnqueuedTime() {
        return enqueuedTime;
    }

    public void setEnqueuedTime(long enqueuedTime) {
        this.enqueuedTime = enqueuedTime;
    }

    public long getDequeuedTime() {
        return dequeuedTime;
    }

    public void setDequeuedTime(long dequeuedTime) {
        this.dequeuedTime = dequeuedTime;
    }

    public void setProcessingFinishedTime(long processingFinishedTime) { this.processingFinishedTime = processingFinishedTime; }

    public long getProcessingFinishedTime() { return processingFinishedTime; }

    public int getMetadataCacheHits() {
        return metadataCacheHits;
    }

    public boolean isDuplicate() {
        return isDuplicate;
    }

    public int getMetadataCacheMisses() {
        return metadataCacheMisses;
    }

    /**
     * Encapsulates the mapping of a particular file or directory to a particular serverless function.
     */
    public static class ServerlessFunctionMapping implements Serializable {
        private static final long serialVersionUID = 7649887040567903783L;

        /**
         * The file or directory that we're mapping to a serverless function.
         */
        public String fileOrDirectory;

        /**
         * The ID of the file or directory's parent iNode.
         */
        public long parentId;

        /**
         * The number of the serverless function to which the file or directory was mapped.
         */
        public int mappedFunctionNumber;

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
