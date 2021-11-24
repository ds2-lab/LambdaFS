package io.hops.metrics;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keep track of how long we perform(ed) various activities.
 */
public class ServerlessNameNodeMetrics implements Serializable {
    /**
     * We map request IDs to ServerlessNameNodeMetrics instances.
     */
    private static ConcurrentHashMap<String, ServerlessNameNodeMetrics> metricsInstances;

    private static final long serialVersionUID = -6462121456311717829L;

    /**
     * Time spent initializing the NameNode. Only occurs during cold starts.
     */
    private volatile long initializationTime;

    /**
     * Time spent writing data to intermediate storage.
     */
    private volatile long intermediateStorageReadTime;

    /**
     * Time spent reading data from intermediate storage.
     */
    private volatile long intermediateStorageWriteTime;

    /**
     * Get the instance of this class associated with the provided requestId.
     * This will create one if an instance does not yet exist.
     * @param requestId The request ID for which the associated ServerlessNameNodeMetrics instance will be returned.
     * @return The ServerlessNameNodeMetrics instance associated with the provided request ID.
     */
    public ServerlessNameNodeMetrics getInstance(String requestId) {
        return metricsInstances.computeIfAbsent(requestId, key -> new ServerlessNameNodeMetrics());
    }

    private ServerlessNameNodeMetrics() {

    }

    /**
     * Reset the metrics instance back to base state.
     */
    public synchronized void resetInstance() {

    }

    public synchronized long getIntermediateStorageWriteTime() {
        return intermediateStorageWriteTime;
    }

    public synchronized long getIntermediateStorageReadTime() {
        return intermediateStorageReadTime;
    }

    public synchronized long getInitializationTime() {
        return initializationTime;
    }

    /**
     * Increment the internal counter for how long we've spent initializing the NameNode.
     *
     * @param amount the amount by which to increment the counter.
     */
    public synchronized void incrementInitializationTime(long amount) {
        this.initializationTime += amount;
    }

    /**
     * Increment the internal counter for how long we've spent reading data from intermediate storage.
     *
     * @param amount the amount by which to increment the counter.
     */
    public synchronized void incrementIntermediateStorageReadTime(long amount) {
        this.initializationTime += amount;
    }

    /**
     * Increment the internal counter for how long we've spent writing data to intermediate storage.
     *
     * @param amount the amount by which to increment the counter.
     */
    public synchronized void incrementIntermediateStorageWriteTime(long amount) {
        this.initializationTime += amount;
    }
}

