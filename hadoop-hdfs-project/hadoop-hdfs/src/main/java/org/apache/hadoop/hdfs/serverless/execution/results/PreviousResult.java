package org.apache.hadoop.hdfs.serverless.execution.results;

import java.io.Serializable;

/**
 * Used to wrap a previously-computed result by the worker thread.
 *
 * The worker thread holds onto previously-computed results for a configurable length of time before
 * discarding them. The results are held onto in case the client does not receive the result. Clients may
 * re-submit the FS operation in this scenario, in which case the worker thread will check if it still has
 * the result in-memory. If it does, it will return it. If it doesn't, then it will return nothing, and the
 * client must simply restart the operation from scratch.
 *
 * For now, the worker thread would not re-perform the given operation was FS operations are obviously not
 * ephemeral and redoing the same operation more than once could have undesirable side effects in some cases.
 *
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class PreviousResult implements Comparable<PreviousResult> {
    /**
     * The value that was returned by the operation.
     */
    public final Serializable value;

    /**
     * The time that the result was posted back to the main thread by the worker thread.
     */
    public long timestamp;

    /**
     * The name of the operation that was performed to generate this result.
     */
    public final String operationName;

    /**
     * The unique identifier of the original TCP/HTTP request(s) that submitted the operation that generated
     * this result.
     */
    public final String requestId;

    /**
     * Create a new instance of PreviousResult.
     *
     * @param value The result of a particular file system operation.
     * @param timestamp The time at which this result was created.
     */
    public PreviousResult(Serializable value, String operationName, String requestId, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
        this.operationName = operationName;
        this.requestId = requestId;
    }

    /**
     * Create a new instance of PreviousResult. This constructor populates the timestamp field automatically.
     * @param value The result of a particular file system operation.
     */
    public PreviousResult(Serializable value, String operationName, String requestId) {
        this(value, operationName, requestId, System.currentTimeMillis());
    }

    /**
     * Get the time elapsed, in milliseconds, since this PreviousResult instance was created.
     */
    public long getTimeElapsedMilliseconds() {
        long now = System.currentTimeMillis();
        return now - timestamp;
    }

    /**
     * This should be called if this result is re-submitted back to the client for one reason or another.
     * This will cause the worker thread to retain this result in-memory for longer. Basically, it just resets
     * the counter on when this result will be purged from memory.
     */
    public void updateTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "PreviousResult (value=" + value.toString() + ", timestamp=" + timestamp + ")";
    }

    /**
     * Note: this class has a natural ordering that is inconsistent with equals.
     */
    @Override
    public int compareTo(PreviousResult other) {
        return Long.compare(this.timestamp, other.timestamp);
    }
}
