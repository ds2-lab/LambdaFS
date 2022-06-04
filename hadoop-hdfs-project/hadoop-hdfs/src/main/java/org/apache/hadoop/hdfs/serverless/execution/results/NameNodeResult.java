package org.apache.hadoop.hdfs.serverless.execution.results;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.serverless.cache.InMemoryINodeCache;
import org.apache.hadoop.hdfs.serverless.cache.MetadataCacheManager;
import org.apache.hadoop.hdfs.serverless.cache.ReplicaCacheManager;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.nustaq.serialization.FSTConfiguration;

import java.io.Serializable;
import java.util.ArrayList;

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
public class NameNodeResult implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(NameNodeResult.class);

    /**
     * From RuedigerMoeller/fast-serialization.
     */
    protected static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    private static final long serialVersionUID = -8990189826253622044L;

    // One easy and important optimization is to register classes which are serialized for sure in
    // your application at the FSTConfiguration object. This way FST can avoid writing class names.
    static {
        conf.registerClass(LocatedBlocks.class, TransactionEvent.class, TransactionAttempt.class, NamespaceInfo.class,
                LastBlockWithStatus.class, HdfsFileStatus.class, DirectoryListing.class, FsServerDefaults.class,
                ActiveServerlessNameNodeList.class, ActiveServerlessNameNode.class);
    }

    /**
     * Exceptions encountered during the current request's execution.
     */
    protected ArrayList<Throwable> exceptions = new ArrayList<>();

    /**
     * Name of the FS operation we performed.
     */
    protected String operationName;

    /**
     * The actual result of the current request's execution.
     */
    protected Serializable result;

    /**
     * Indicates whether this result corresponds to a duplicate request.
     */
    protected boolean isDuplicate = false;

    /**
     * Request ID associated with this result.
     */
    protected String requestId;

    /**
     * Flag which indicates whether there is a result.
     */
    protected boolean hasResult = false;

    /**
     * We may be returning a mapping of a file or directory to a particular serverless function.
     */
    protected ServerlessFunctionMapping serverlessFunctionMapping;

    public NameNodeResult(String requestId, String operationName) {
        this.requestId = requestId;
        this.operationName = operationName;
    }

    protected NameNodeResult() { }

    public boolean isDuplicate() {
        return isDuplicate;
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
     * Update the result field of this request's execution.
     * @param result The result of executing the desired operation.
     * @param forceOverwrite If true, overwrite an existing result.
     * @return True if the result was stored, otherwise false.
     */
    public boolean addResult(Serializable result, boolean forceOverwrite) {
        if (result == null || forceOverwrite) {
            this.result = result;
            this.hasResult = true;
            
            return true;
        } else {
            LOG.warn("Cannot overwrite existing result of type " + result.getClass().getSimpleName() + ".");
        }

        return false;
    }

    public Serializable getResult() {
        return result;
    }

    public ServerlessFunctionMapping getServerlessFunctionMapping() {
        return serverlessFunctionMapping;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getOperationName() {
        return operationName;
    }

    /**
     * Formally record/take note that an exception occurred.
     * @param ex The exception that occurred.
     */
    public void addException(Exception ex) {
        addThrowable(ex);
    }

    public ArrayList<Throwable> getExceptions() {
        return exceptions;
    }

    public boolean hasResult() {
        return hasResult;
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

    public String serializeAndEncode(Object object) {
        byte[] objectBytes = conf.asByteArray(object);
        return Base64.encodeBase64String(objectBytes);
    }

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

        json.addProperty(OPERATION, operationName);

        // Reset these in-case this thread gets re-used in the future for another request.
        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();

        json.addProperty(FN_END_TIME, System.currentTimeMillis());
        return json;
    }

    /**
     * Called before sending the result via TCP.
     *
     * This should NOT be called by any derived classes, as we reset the hit/miss counters.
     */
    public void prepare(MetadataCacheManager metadataCacheManager) {
        InMemoryINodeCache metadataCache = metadataCacheManager.getINodeCache();
        ReplicaCacheManager replicaCacheManager = metadataCacheManager.getReplicaCacheManager();

        if (result instanceof DuplicateRequest)
            isDuplicate = true;

        metadataCache.resetCacheHitMissCounters();
        replicaCacheManager.resetCacheHitMissCounters();
    }
}
