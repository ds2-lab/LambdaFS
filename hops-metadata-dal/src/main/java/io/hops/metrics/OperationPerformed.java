package io.hops.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

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

    public static final String INVOCATION_TIME =        "INVOCATION TIME";
    public static final String PREPROCESSING_TIME =     "PREPROCESSING TIME";
    public static final String WAITING_IN_QUEUE =       "WAITING IN QUEUE";
    public static final String EXECUTION_TIME =         "EXECUTION TIME";
    public static final String POSTPROCESSING_TIME =    "POSTPROCESSING TIME";
    public static final String RETURNING_TO_USER =      "RETURNING TO USER";

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
     * The duration of the serverless function itself, not including
     * invocation overhead.
     */
    private long serverlessFunctionDuration;

    private final int deployment;

    private final boolean issuedViaTcp;

    private final boolean issuedViaHttp;

    private long nameNodeId;

    /**
     * Indicates whether the result was ultimately received via HTTP or TCP.
     */
    private String resultReceivedVia;

    /////////////////////////////
    // TIMING/DURATION METRICS //
    /////////////////////////////

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
     * Time at which in-memory processing/execution of the requested file system operation finished.
     */
    private long resultFinishedProcessingTime;

    public OperationPerformed(String operationName, String requestId,
                              long invokedAtTime, long resultReceivedTime,
                              long enqueuedTime, long dequeuedTime,
                              long serverlessFnStartTime, long serverlessFnEndTime,
                              int deployment, boolean issuedViaHttp,
                              boolean issuedViaTcp, String resultReceivedVia,
                              long nameNodeId, int metadataCacheMisses,
                              int metadataCacheHits) {
        this.operationName = operationName;
        this.requestId = requestId;
        this.invokedAtTime = invokedAtTime;
        this.requestEnqueuedAtTime = enqueuedTime;
        this.resultReceivedTime = resultReceivedTime;
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
        this.resultReceivedVia = resultReceivedVia;
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
                "finished_executing_time,serverless_fn_end_time,result_received_time,invocation_duration," +
                "preprocessing_duration,waiting_in_queue_duration,execution_duration,postprocessing_duration,return_to_client_duration," +
                "serverless_fn_duration,deployment_number,name_node_id,request_type,metadata_cache_hits,metadata_cache_misses";
    }

    @Override
    public String toString() {
        String formatString = "%-16s %-38s %-26s %-26s %-26s %-26s %-26s %-26s %-8s %-3s %-22s %-6s %-5s %-5s";
        return String.format(formatString, operationName, requestId,
                Instant.ofEpochMilli(invokedAtTime).toString(),             // Client invokes NN.
                Instant.ofEpochMilli(serverlessFnStartTime).toString(),     // NN begins executing.
                Instant.ofEpochMilli(requestEnqueuedAtTime).toString(),     // NN enqueues req. in work queue.
                Instant.ofEpochMilli(resultBeganExecutingTime).toString(),  // NN dequeues req. from work queue, begins executing it.
                Instant.ofEpochMilli(serverlessFnEndTime).toString(),       // NN returns result to client.
                Instant.ofEpochMilli(resultReceivedTime).toString(),        // Client receives result from NN.
                serverlessFunctionDuration, deployment, nameNodeId,
                resultReceivedVia, metadataCacheHits, metadataCacheMisses);
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
        // "operation_name,request_id,invoked_at_time,serverless_fn_start_time,enqueued_at_time,began_executing_time," +
        // "finished_executing_time,serverless_fn_end_time,result_received_time,invocation_duration,preprocessing_duration," +
        // "waiting_in_queue_duration,execution_duration,postprocessing_duration,return_to_client_duration,serverless_fn_duration," +
        // "deployment_number,name_node_id,request_type,metadata_cache_hits,metadata_cache_misses"
        String formatString = "%-16s,%-38s," +                                  // 2
                              "%-26s,%-26s,%-26s,%-26s,%-26s,%-26s,%-26s," +    // 7
                              "%-8s,%-8s,%-8s,%-8s,%-8s,%-8s,%-8s," +           // 7
                              "%-3s,%-22s,%-6s,%-5s,%-5s";                      // 5
        writer.write(String.format(formatString,
                operationName, requestId,
                invokedAtTime,                    // Client invokes NN.
                serverlessFnStartTime,            // NN begins executing.
                requestEnqueuedAtTime,            // NN enqueues req. in work queue.
                resultBeganExecutingTime,         // NN dequeues req. from work queue, begins executing it.
                resultFinishedProcessingTime,     // NN finished executing the requested operation.
                serverlessFnEndTime,              // NN returns result to client.
                resultReceivedTime,               // Client receives result from NN.
                serverlessFnStartTime - invokedAtTime,
                requestEnqueuedAtTime - serverlessFnStartTime,
                resultBeganExecutingTime - requestEnqueuedAtTime,
                resultFinishedProcessingTime - resultBeganExecutingTime,
                serverlessFnEndTime - resultFinishedProcessingTime,
                resultReceivedTime - serverlessFnEndTime,
                serverlessFunctionDuration,
                deployment, nameNodeId, resultReceivedVia, metadataCacheHits, metadataCacheMisses));
        writer.newLine();
    }

    public String getResultReceivedVia() {
        return resultReceivedVia;
    }

    public void setResultReceivedVia(String resultReceivedVia) {
        this.resultReceivedVia = resultReceivedVia;
    }

    public long getResultFinishedProcessingTime() {
        return resultFinishedProcessingTime;
    }

    public void setResultFinishedProcessingTime(long resultFinishedProcessingTime) {
        this.resultFinishedProcessingTime = resultFinishedProcessingTime;
    }

    /**
     * Print the averages of the following metrics:
     *      INVOCATION TIME,
     *      PREPROCESSING TIME,
     *      WAITING IN QUEUE,
     *      EXECUTION TIME,
     *      POSTPROCESSING TIME,
     *      RETURNING TO USER.
     *
     * @param collection Collection of {@link OperationPerformed} instances for which the average values of each of the
     *                   above metrics should be computed.
     */
    public static HashMap<String, Double> getAverages(Collection<OperationPerformed> collection) {
        HashMap<String, Integer> counts = new HashMap<>();
        HashMap<String, Long> sums = getSumsAndCounts(collection, counts);
        HashMap<String, Double> averages = new HashMap<>();

        for (Map.Entry<String, Long> entry : sums.entrySet()) {
            averages.put(entry.getKey(), (double)entry.getValue() / (double)counts.get(entry.getKey()));
        }

        return averages;
    }

    /**
     * Print the sums of the following metrics:
     *      INVOCATION TIME,
     *      PREPROCESSING TIME,
     *      WAITING IN QUEUE,
     *      EXECUTION TIME,
     *      POSTPROCESSING TIME,
     *      RETURNING TO USER.
     *
     * @param collection Collection of {@link OperationPerformed} instances for which the sums of each of the above
     *                   metrics should be computed.
     */
    public static HashMap<String, Long> getSums(Collection<OperationPerformed> collection) {
        return getSumsAndCounts(collection, new HashMap<>()); // Don't care about the counts.
    }

    /**
     *
     * @param collection Collection of {@link OperationPerformed} instances for which the sums of each of the above
     *                   metrics should be computed.
     * @param counts     The number of values added together to compute the sum for each metric.
     */
    public static HashMap<String, Long> getSumsAndCounts(Collection<OperationPerformed> collection,
                                                         HashMap<String, Integer> counts) {
        HashMap<String, Long> sums = new HashMap<>();
        sums.put(INVOCATION_TIME, 0L);
        sums.put(PREPROCESSING_TIME, 0L);
        sums.put(WAITING_IN_QUEUE, 0L);
        sums.put(EXECUTION_TIME, 0L);
        sums.put(POSTPROCESSING_TIME, 0L);
        sums.put(RETURNING_TO_USER, 0L);

        for (OperationPerformed op : collection) {
            long invocationTime = sums.get(INVOCATION_TIME);
            long preprocessingTime = sums.get(PREPROCESSING_TIME);
            long waitingInQueue = sums.get(WAITING_IN_QUEUE);
            long executionTime = sums.get(EXECUTION_TIME);
            long postprocessingTime = sums.get(POSTPROCESSING_TIME);
            long returningToUser = sums.get(RETURNING_TO_USER);

            if (op.serverlessFnStartTime - op.invokedAtTime > 0) {
                sums.put(INVOCATION_TIME, invocationTime + (op.serverlessFnStartTime - op.invokedAtTime));
                counts.merge(INVOCATION_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            if (op.requestEnqueuedAtTime - op.serverlessFnStartTime > 0) {
                sums.put(PREPROCESSING_TIME, preprocessingTime + (op.requestEnqueuedAtTime - op.serverlessFnStartTime));
                counts.merge(PREPROCESSING_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            if (op.resultBeganExecutingTime - op.requestEnqueuedAtTime > 0) {
                sums.put(WAITING_IN_QUEUE, waitingInQueue + (op.resultBeganExecutingTime - op.requestEnqueuedAtTime));
                counts.merge(WAITING_IN_QUEUE, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            if (op.resultFinishedProcessingTime - op.resultBeganExecutingTime > 0) {
                sums.put(EXECUTION_TIME, executionTime + (op.resultFinishedProcessingTime - op.resultBeganExecutingTime));
                counts.merge(EXECUTION_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            if (op.serverlessFnEndTime - op.resultFinishedProcessingTime > 0) {
                sums.put(POSTPROCESSING_TIME, postprocessingTime + (op.serverlessFnEndTime - op.resultFinishedProcessingTime));
                counts.merge(POSTPROCESSING_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            if (op.resultReceivedTime - op.serverlessFnEndTime > 0) {
                sums.put(RETURNING_TO_USER, returningToUser + (op.resultReceivedTime - op.serverlessFnEndTime));
                counts.merge(RETURNING_TO_USER, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
        }

        return sums;
    }

    public static String getMetricsHeader() {
        return String.format("%-20s %-20s %-20s %-20s %-20s %-20s",
                INVOCATION_TIME, PREPROCESSING_TIME, WAITING_IN_QUEUE,
                EXECUTION_TIME, POSTPROCESSING_TIME, RETURNING_TO_USER);
    }

    /**
     * Print a HashMap of either average or sum values for the following metrics:
     *      INVOCATION TIME,
     *      PREPROCESSING TIME,
     *      WAITING IN QUEUE,
     *      EXECUTION TIME,
     *      POSTPROCESSING TIME,
     *      RETURNING TO USER.
     */
    public static String getMetricsString(HashMap<String, ?> metrics) {
         return String.format("%-20s %-20s %-20s %-20s %-20s %-20s",
                metrics.get(INVOCATION_TIME), metrics.get(PREPROCESSING_TIME), metrics.get(WAITING_IN_QUEUE),
                 metrics.get(EXECUTION_TIME), metrics.get(POSTPROCESSING_TIME), metrics.get(RETURNING_TO_USER));
    }
}
