package org.apache.hadoop.hdfs.serverless.execution.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.context.TransactionsStats;
import org.apache.hadoop.hdfs.serverless.cache.InMemoryINodeCache;
import org.apache.hadoop.hdfs.serverless.cache.MetadataCacheManager;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;

public final class NameNodeResultWithMetrics extends NameNodeResult {
    public static final Logger LOG = LoggerFactory.getLogger(NameNodeResultWithMetrics.class);
    private static final long serialVersionUID = -6018521672360252605L;

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
     * The name of the serverless function all of this is running in/on.
     */
    private int deploymentNumber;

    /**
     * The unique ID of the current NameNode instance.
     */
    private long nameNodeId;

    /**
     * Indicates whether the NameNode was cold-started when computing this result.
     * Only used for HTTP, as TCP cannot be used except with warm NameNodes.
     */
    private boolean coldStart = false;

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

    /**
     * The approximate number of collections that occurred while the NN was executing the task associated with this
     * NameNodeResult instance.
     *
     * This initially set to the number of GCs that have occurred up until the point at which the NN begins
     * executing the task associated with this NameNodeResult instance. This value is then used to compute the
     * number of collections that have occurred while the NN executed the associated task.
     */
    private long numGarbageCollections;

    /**
     * The approximate time, in milliseconds, that has elapsed during GCs while the NN executed this task.
     *
     * This initially set to the elapsed time until the point at which the NN begins executing the task associated with
     * this NameNodeResult instance. This value is then used to compute the elapsed time during collections while the
     * NN executed the associated task.
     */
    private long garbageCollectionTime;

    public NameNodeResultWithMetrics(int deploymentNumber, String requestId, String requestMethod, long nameNodeId, String operationName) {
        super(requestId, operationName);
        this.deploymentNumber = deploymentNumber;
        this.nameNodeId = nameNodeId;
        this.exceptions = new ArrayList<>();
        this.requestMethod = requestMethod;

        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.numGarbageCollections = 0L;
        this.garbageCollectionTime = 0L;
        for (GarbageCollectorMXBean mxBean : mxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                this.numGarbageCollections += count;

            if (time > 0)
                this.garbageCollectionTime += time;
        }
    }

    // Empty constructor used for Kryo serialization.
    private NameNodeResultWithMetrics() {super(); }

    public long getNumGarbageCollections() { return numGarbageCollections; }

    public long getGarbageCollectionTime() { return garbageCollectionTime; }

    public void setColdStart(boolean coldStart) {
        this.coldStart = coldStart;
    }

    /**
     * Log some useful debug information about the result field (e.g., the object's type) to the console.
     */
    public void logResultDebugInformation(String opPerformed) {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug("+-+-+-+-+-+-+ Result Debug Information +-+-+-+-+-+-+");
        LOG.debug("Request ID: " + requestId);
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
            json.put(DUPLICATE_REQUEST, true);
        } else {
            if (result != null && LOG.isDebugEnabled())
                LOG.debug("Returning result of type " + result.getClass().getSimpleName()
                        + " to client for request " + requestId + ".");

            String resultSerializedAndEncoded = serializeAndEncode(result);

            if (resultSerializedAndEncoded != null)
                json.put(RESULT, resultSerializedAndEncoded);
            json.put(DUPLICATE_REQUEST, false);
        }

        if (exceptions.size() > 0) {
            ArrayNode exceptionsJson = mapper.createArrayNode();

            for (Throwable t : exceptions) {
                exceptionsJson.add(t.toString());
            }

            json.set(EXCEPTIONS, exceptionsJson);
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
            json.put(OPERATION, operation);

        long numGarbageCollectionsNow = 0L;
        long garbageCollectionTimeNow = 0L;
        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean mxBean : mxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                numGarbageCollectionsNow += count;

            if (time > 0)
                garbageCollectionTimeNow += time;
        }

        numGarbageCollections = numGarbageCollectionsNow - numGarbageCollections;
        garbageCollectionTime = garbageCollectionTimeNow - garbageCollectionTime;

        json.put(NAME_NODE_ID, nameNodeId);
        json.put(DEPLOYMENT_NUMBER, deploymentNumber);
        json.put(REQUEST_ID, requestId);
        json.put(REQUEST_METHOD, requestMethod);
        json.put(CANCELLED, false);
        json.put(OPENWHISK_ACTIVATION_ID, "N/A"); //System.getenv("__OW_ACTIVATION_ID"));
        int totalCacheHits = metadataCache.getNumCacheHitsCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheHits();
        int totalCacheMisses = metadataCache.getNumCacheMissesCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheMisses();
        json.put(CACHE_HITS, totalCacheHits);
        json.put(CACHE_MISSES, totalCacheMisses);
        json.put(FN_START_TIME, fnStartTime);
        json.put(ENQUEUED_TIME, enqueuedTime);
        json.put(DEQUEUED_TIME, dequeuedTime);
        json.put(PROCESSING_FINISHED_TIME, processingFinishedTime);
        json.put(COLD_START, coldStart);
        json.put(NUMBER_OF_GCs, numGarbageCollections);
        json.put(GC_TIME, garbageCollectionTime);

        if (statisticsPackageSerializedAndEncoded != null)
            json.put(STATISTICS_PACKAGE, statisticsPackageSerializedAndEncoded);

        if (txEventsSerializedAndEncoded != null)
            json.put(TRANSACTION_EVENTS, txEventsSerializedAndEncoded);

        // Reset these in-case this thread gets re-used in the future for another request.
        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        json.put(FN_END_TIME, System.currentTimeMillis());
        return json;
    }

    /**
     * Called before sending the result via TCP.
     */
    @Override
    public void prepare(MetadataCacheManager metadataCacheManager) {
        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        if (result instanceof DuplicateRequest)
            isDuplicate = true;

        metadataCacheHits = metadataCache.getNumCacheHitsCurrentRequest();      // + replicaCacheManager.getThreadLocalCacheHits();
        metadataCacheMisses = metadataCache.getNumCacheMissesCurrentRequest();  // + replicaCacheManager.getThreadLocalCacheMisses();

        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        long numGarbageCollectionsNow = 0L;
        long garbageCollectionTimeNow = 0L;
        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean mxBean : mxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                numGarbageCollectionsNow += count;

            if (time > 0)
                garbageCollectionTimeNow += time;
        }

        numGarbageCollections = numGarbageCollectionsNow - numGarbageCollections;
        garbageCollectionTime = garbageCollectionTimeNow - garbageCollectionTime;

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
     */
    @Override
    public JsonObject toJson(MetadataCacheManager metadataCacheManager) {
        JsonObject json = new JsonObject();
        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        // If the result is a duplicate request, then don't bother sending an actual result field.
        // That's just unnecessary network I/O. We can just include a flag indicating that this is
        // a duplicate request and leave it at that.
        if (result instanceof DuplicateRequest) {
            // Add a flag indicating whether this is just a duplicate result.
            json.addProperty(DUPLICATE_REQUEST, true);
        } else {
            if (result != null && LOG.isDebugEnabled())
                LOG.debug("Returning result of type " + result.getClass().getSimpleName()
                        + " to client. Result value: " + result.toString());

            String resultSerializedAndEncoded = serializeAndEncode(result);

            if (resultSerializedAndEncoded != null)
                json.addProperty(RESULT, resultSerializedAndEncoded);
            json.addProperty(DUPLICATE_REQUEST, false);
        }

        if (exceptions.size() > 0) {
            JsonArray exceptionsJson = new JsonArray();

            for (Throwable t : exceptions) {
                exceptionsJson.add(t.toString());
            }

            json.add(EXCEPTIONS, exceptionsJson);
        }

        if (serverlessFunctionMapping != null) {
            // Embed all the information about the serverless function mapping in the Json response.
            JsonObject functionMapping = new JsonObject();
            functionMapping.addProperty(FILE_OR_DIR, serverlessFunctionMapping.fileOrDirectory);
            functionMapping.addProperty(PARENT_ID, serverlessFunctionMapping.parentId);
            functionMapping.addProperty(FUNCTION, serverlessFunctionMapping.mappedFunctionNumber);

            json.add(DEPLOYMENT_MAPPING, functionMapping);
        }

        long numGarbageCollectionsNow = 0L;
        long garbageCollectionTimeNow = 0L;
        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean mxBean : mxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                numGarbageCollectionsNow += count;

            if (time > 0)
                garbageCollectionTimeNow += time;
        }

        numGarbageCollections = numGarbageCollectionsNow - numGarbageCollections;
        garbageCollectionTime = garbageCollectionTimeNow - garbageCollectionTime;

        json.addProperty(NAME_NODE_ID, nameNodeId);
        json.addProperty(DEPLOYMENT_NUMBER, deploymentNumber);
        json.addProperty(REQUEST_ID, requestId);
        json.addProperty(REQUEST_METHOD, requestMethod);
        json.addProperty(CANCELLED, false);
        json.addProperty(OPENWHISK_ACTIVATION_ID, System.getenv("__OW_ACTIVATION_ID"));
        int totalCacheHits = metadataCache.getNumCacheHitsCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheHits();
        int totalCacheMisses = metadataCache.getNumCacheMissesCurrentRequest(); // + replicaCacheManager.getThreadLocalCacheMisses();
        json.addProperty(CACHE_HITS, totalCacheHits);
        json.addProperty(CACHE_MISSES, totalCacheMisses);
        json.addProperty(FN_START_TIME, fnStartTime);
        json.addProperty(ENQUEUED_TIME, enqueuedTime);
        json.addProperty(DEQUEUED_TIME, dequeuedTime);
        json.addProperty(PROCESSING_FINISHED_TIME, processingFinishedTime);
        json.addProperty(COLD_START, coldStart);
        json.addProperty(NUMBER_OF_GCs, numGarbageCollections);
        json.addProperty(GC_TIME, garbageCollectionTime);
        json.addProperty(OPERATION, operationName);

        if (statisticsPackageSerializedAndEncoded != null)
            json.addProperty(STATISTICS_PACKAGE, statisticsPackageSerializedAndEncoded);

        if (txEventsSerializedAndEncoded != null)
            json.addProperty(TRANSACTION_EVENTS, txEventsSerializedAndEncoded);

        // Reset these in-case this thread gets re-used in the future for another request.
        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        json.addProperty(FN_END_TIME, System.currentTimeMillis());
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

    public int getDeploymentNumber() {
        return deploymentNumber;
    }

    public long getNameNodeId() {
        return nameNodeId;
    }

    public boolean isColdStart() {
        return coldStart;
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
        return "NameNodeResultWithMetrics(RequestID=" + requestId + ", OperationName=" + operationName + ")";
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

    public int getMetadataCacheMisses() {
        return metadataCacheMisses;
    }
}
