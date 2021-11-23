package org.apache.hadoop.hdfs.serverless.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;

// "Op Name", "Start Time", "End Time", "Duration (ms)", "Deployment", "HTTP", "TCP"
public class OperationPerformed implements Serializable, Comparable<OperationPerformed> {
    /**
     * Compare OperationPerformed instances by their start time.
     */
    public static Comparator<OperationPerformed> BY_START_TIME = new Comparator<OperationPerformed>() {
        @Override
        public int compare(OperationPerformed o1, OperationPerformed o2) {
            return (int)(o1.serverlessFnStartTime - o2.serverlessFnStartTime);
        }
    };

    /**
     * Compare OperationPerformed instances by their end time.
     */
    public static Comparator<OperationPerformed> BY_END_TIME = new Comparator<OperationPerformed>() {
        @Override
        public int compare(OperationPerformed o1, OperationPerformed o2) {
            return (int)(o1.serverlessFnEndTime - o2.serverlessFnEndTime);
        }
    };

    /**
     * Number of INode cache hits that the NameNode encountered while processing the associated request.
     */
    private int metadataCacheHits;

    /**
     * Number of INode cache hits that the NameNode encountered while processing the associated request.
     */
    private int metadataCacheMisses;

    private static final long serialVersionUID = -3094538262184661023L;

    private final String operationName;

    private final String requestId;

    /**
     * The time at which the client issued the invocation for the serverless function.
     */
    private long invokedAtTime;

    /**
     * The time at which the serverless function began executing.
     */
    private long serverlessFnStartTime;

    /**
     * The time at which the serverless function finished executing.
     */
    private long serverlessFnEndTime;

    /**
     * Duration starting with the client invoking the NameNode and ending with
     * the client receiving a result from the NameNode. Includes OpenWhisk overhead
     * and whatever else.
     */
    private long endToEndDuration;

    /**
     * The time at which the client received the HTTP response from the
     * serverless function.
     */
    private long resultReceivedTime;

    /**
     * Time at which the function was enqueued in the NameNode's work queue.
     */
    private long requestEnqueuedAtTime;

    /**
     * Time at which the function began executing on the NameNode.
     */
    private long resultBeganExecutingTime;

    /**
     * The duration of the serverless function itself, not including
     * invocation overhead.
     */
    private long serverlessFunctionDuration;

    private final int deployment;

    private final boolean issuedViaTcp;

    private final boolean issuedViaHttp;

    private long nameNodeId;

    public OperationPerformed(String operationName, String requestId,
                              long invokedAtTime, long resultReceivedTime,
                              long enqueuedTime, long dequeuedTime,
                              long serverlessFnStartTime, long serverlessFnEndTime,
                              int deployment, boolean issuedViaHttp,
                              boolean issuedViaTcp, long nameNodeId,
                              int metadataCacheMisses, int metadataCacheHits) {
        this.operationName = operationName;
        this.requestId = requestId;
        this.invokedAtTime = invokedAtTime;
        this.requestEnqueuedAtTime = enqueuedTime;
        this.resultBeganExecutingTime = dequeuedTime;
        this.serverlessFnStartTime = serverlessFnStartTime;
        this.serverlessFnEndTime = serverlessFnEndTime;
        this.serverlessFunctionDuration = serverlessFnEndTime - serverlessFnStartTime;
        this.endToEndDuration = invokedAtTime - resultReceivedTime;
        this.deployment = deployment;
        this.issuedViaHttp = issuedViaHttp;
        this.issuedViaTcp = issuedViaTcp;
        this.nameNodeId = nameNodeId;
        this.metadataCacheHits = metadataCacheHits;
        this.metadataCacheMisses = metadataCacheMisses;
    }

    public void setNameNodeId(long nameNodeId) {
        this.nameNodeId = nameNodeId;
    }

    /**
     * Modify the timestamp at which the serverless function finished executing.
     * This also recomputes this instance's `duration` field.
     *
     * @param serverlessFnEndTime The end time in milliseconds.
     */
    public void setServerlessFnEndTime(long serverlessFnEndTime) {
        this.serverlessFnEndTime = serverlessFnEndTime;
        this.serverlessFunctionDuration = this.serverlessFnEndTime - serverlessFnStartTime;
    }

    /**
     * Modify the timestamp at which the serverless function started executing.
     * This also recomputes this instance's `duration` field.
     *
     * @param serverlessFnStartTime The end time in milliseconds.
     */
    public void setServerlessFnStartTime(long serverlessFnStartTime) {
        this.serverlessFnStartTime = serverlessFnStartTime;
        this.serverlessFunctionDuration = this.serverlessFnEndTime - this.serverlessFnStartTime;
    }

    public Object[] getAsArray() {
        return new Object[] {
                this.operationName, this.serverlessFnStartTime, this.serverlessFnEndTime, this.serverlessFunctionDuration, this.deployment,
                this.issuedViaHttp, this.issuedViaTcp
        };
    }

    /**
     * Return the header for the CSV file.
     */
    public static String getHeader() {
        return "operation_name,request_id,invoked_at_time,serverless_fn_start_time,enqueued_at_time,began_executing_time," +
                "serverless_fn_end_time,result_received_time,serverless_fn_duration,deployment_number," +
                "name_node_id,metadata_cache_hits,metadata_cache_misses";
    }

    @Override
    public String toString() {
        String formatString = "%-16s %-38s %-26s %-26s %-26s %-26s %-26s %-26s %-8s %-3s %-22s %-5s %-5s";
        return String.format(formatString, operationName, requestId,
                Instant.ofEpochMilli(invokedAtTime).toString(),             // Client invokes NN.
                Instant.ofEpochMilli(serverlessFnStartTime).toString(),     // NN begins executing.
                Instant.ofEpochMilli(requestEnqueuedAtTime).toString(),     // NN enqueues req. in work queue.
                Instant.ofEpochMilli(resultBeganExecutingTime).toString(),  // NN dequeues req. from work queue, begins executing it.
                Instant.ofEpochMilli(serverlessFnEndTime).toString(),       // NN returns result to client.
                Instant.ofEpochMilli(resultReceivedTime).toString(),        // Client receives result from NN.
                serverlessFunctionDuration, deployment, nameNodeId,
                metadataCacheHits, metadataCacheMisses);
    }

    /**
     * Compare two instances of OperationPerformed.
     * The comparison is based exclusively on their timeIssued field.
     */
    @Override
    public int compareTo(OperationPerformed op) {
        return Long.compare(serverlessFnEndTime, op.serverlessFnEndTime);
    }

    public String getRequestId() {
        return requestId;
    }

    public int getMetadataCacheMisses() {
        return metadataCacheMisses;
    }

    public void setMetadataCacheMisses(int metadataCacheMisses) {
        this.metadataCacheMisses = metadataCacheMisses;
    }

    public int getMetadataCacheHits() {
        return metadataCacheHits;
    }

    public void setMetadataCacheHits(int metadataCacheHits) {
        this.metadataCacheHits = metadataCacheHits;
    }

    public long getResultReceivedTime() {
        return resultReceivedTime;
    }

    /**
     * Update the timestamp at which a result was received.
     * Also updates the 'endToEndDuration' field.
     */
    public void setResultReceivedTime(long resultReceivedTime) {
        this.resultReceivedTime = resultReceivedTime;
        this.endToEndDuration = this.resultReceivedTime - this.invokedAtTime;
    }

    public long getInvokedAtTime() {
        return invokedAtTime;
    }

    /**
     * Update the timestamp at which the client invoked the NameNode.
     * Also updates the 'endToEndDuration' field.
     */
    public void setInvokedAtTime(long invokedAtTime) {
        this.invokedAtTime = invokedAtTime;
        this.endToEndDuration = this.resultReceivedTime - this.invokedAtTime;
    }

    public long getRequestEnqueuedAtTime() {
        return requestEnqueuedAtTime;
    }

    public void setRequestEnqueuedAtTime(long requestEnqueuedAtTime) {
        this.requestEnqueuedAtTime = requestEnqueuedAtTime;
    }

    public long getResultBeganExecutingTime() {
        return resultBeganExecutingTime;
    }

    public void setResultBeganExecutingTime(long resultBeganExecutingTime) {
        this.resultBeganExecutingTime = resultBeganExecutingTime;
    }

    /**
     * Write this instance to a file in CSV format (using tabs to separate).
     */
    public void write(BufferedWriter writer) throws IOException {
        String formatString = "%-16s,%-38s,%-26s,%-26s,%-26s,%-26s,%-26s,%-26s,%-8s,%-3s,%-22s,%-5s,%-5s";
        writer.write(String.format(formatString, operationName, requestId,
                invokedAtTime,             // Client invokes NN.
                serverlessFnStartTime,     // NN begins executing.
                requestEnqueuedAtTime,     // NN enqueues req. in work queue.
                resultBeganExecutingTime,  // NN dequeues req. from work queue, begins executing it.
                serverlessFnEndTime,       // NN returns result to client.
                resultReceivedTime,        // Client receives result from NN.
                serverlessFunctionDuration, deployment, nameNodeId,
                metadataCacheHits, metadataCacheMisses));
        writer.newLine();
    }
}
