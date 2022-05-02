package io.hops.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
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

    private String operationName;

    private String requestId;

    /**
     * The duration of the serverless function itself, not including
     * invocation overhead.
     */
    private long serverlessFunctionDuration;

    private int deployment;

    private boolean issuedViaTcp;

    private boolean issuedViaHttp;

    private long nameNodeId;

    /**
     * Some sort of unique identifier of the client that issued the operation
     * (e.g., thread ID or the HopsFS client name variable).
     */
    private String clientId;

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

    /**
     * Indicates whether this request was resubmitted via the straggler mitigation technique.
     */
    private boolean stragglerResubmitted;

    /**
     * The approximate number of collections that occurred while the NN was executing the task associated with this
     * OperationPerformed instance.
     *
     * This initially set to the number of GCs that have occurred up until the point at which the NN begins
     * executing the task associated with this OperationPerformed instance. This value is then used to compute the
     * number of collections that have occurred while the NN executed the associated task.
     */
    private long numGarbageCollections;

    /**
     * The approximate time, in milliseconds, that has elapsed during collections while the NN executed this task.
     *
     * This initially set to the elapsed time until the point at which the NN begins executing the task associated with
     * this NameNodeResult instance. This value is then used to compute the elapsed time during collections while the
     * NN executed the associated task.
     */
    private long garbageCollectionTime;

    /**
     * Identifies the JVM from which this OperationPerformed instance originated. It is of the form PID@HOSTNAME.
     */
    private String originJvmIdentifier;

    /**
     * An identifier for the JVM from which this OperationPerformed instance originated.
     *
     * It is of the form PID@HOSTNAME.
     */
    private static final String localJvmName;

    static {
        localJvmName = ManagementFactory.getRuntimeMXBean().getName();
    }

    private OperationPerformed() { }

    public OperationPerformed(String operationName, String requestId,
                              long invokedAtTime, long resultReceivedTime,
                              long enqueuedTime, long dequeuedTime,
                              long serverlessFnStartTime, long serverlessFnEndTime,
                              int deployment, boolean issuedViaHttp,
                              boolean issuedViaTcp, String resultReceivedVia,
                              long nameNodeId, int metadataCacheMisses,
                              int metadataCacheHits, long finishedProcessingAt,
                              boolean stragglerResubmitted, String clientId,
                              long numGarbageCollections, long garbageCollectionTime) {
        this.operationName = operationName;
        this.requestId = requestId;
        this.invokedAtTime = invokedAtTime;
        this.requestEnqueuedAtTime = enqueuedTime;
        this.resultReceivedTime = resultReceivedTime;
        this.resultBeganExecutingTime = dequeuedTime;
        this.serverlessFnStartTime = serverlessFnStartTime;
        this.serverlessFnEndTime = serverlessFnEndTime;
        this.serverlessFunctionDuration = serverlessFnEndTime - serverlessFnStartTime;
        this.endToEndDuration = resultReceivedTime - invokedAtTime;
        this.deployment = deployment;
        this.issuedViaHttp = issuedViaHttp;
        this.issuedViaTcp = issuedViaTcp;
        this.nameNodeId = nameNodeId;
        this.metadataCacheHits = metadataCacheHits;
        this.metadataCacheMisses = metadataCacheMisses;
        this.resultReceivedVia = resultReceivedVia;
        this.resultFinishedProcessingTime = finishedProcessingAt;
        this.stragglerResubmitted = stragglerResubmitted;
        this.clientId = clientId;
        this.numGarbageCollections = numGarbageCollections;
        this.garbageCollectionTime = garbageCollectionTime;
        this.originJvmIdentifier = localJvmName;
    }

    public long getNumGarbageCollections() { return this.numGarbageCollections; }

    public long getGarbageCollectionTime() { return this.garbageCollectionTime; }

    public void setNameNodeId(long nameNodeId) {
        this.nameNodeId = nameNodeId;
    }

    public long getNameNodeId() { return this.nameNodeId; }

    public int getDeployment() { return this.deployment; }

    public boolean getStragglerResubmitted() { return this.stragglerResubmitted; }

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
     * Convert the `stragglerResubmitted` instance variable, which is a boolean, to an int. This is used
     * when writing CSVs, as we can sum integers to quickly get the total number of requests resubmitted via
     * straggler mitigation.
     *
     * @return 1 if stragglerResubmitted is true, else 0.
     */
    private int stragglerResubmittedToInt() {
        if (stragglerResubmitted) return 1;
        return 0;
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
        return "operation_name,request_id,client_id,origin_jvm,invoked_at_time,serverless_fn_start_time,enqueued_at_time,began_executing_time," +
                "finished_executing_time,serverless_fn_end_time,result_received_time,invocation_duration," +
                "preprocessing_duration,waiting_in_queue_duration,execution_duration,postprocessing_duration,return_to_client_duration," +
                "serverless_fn_duration,end_to_end_duration,deployment_number,name_node_id,request_type," +
                "metadata_cache_hits,metadata_cache_misses,straggler_resubmitted,num_gcs,gc_time";
    }

    public static String getToStringHeader() {
        return "OpName RequestID InvokedAt FnStartTime EnqueuedAt BeganExecutingAt FnEndTime ResultReceivedAt FnDuration DepNum NNID ResReceivedVia CacheHits CacheMisses";
    }

    @Override
    public String toString() {
        String formatString = "%-16s %-38s %-26s %-26s %-26s %-26s %-26s %-26s %-26s %-8s %-3s %-22s %-6s %-5s %-5s";
        return String.format(formatString, operationName, requestId, clientId,
                Instant.ofEpochMilli(invokedAtTime).toString(),             // Client invokes NN.
                Instant.ofEpochMilli(serverlessFnStartTime).toString(),     // NN begins executing.
                Instant.ofEpochMilli(requestEnqueuedAtTime).toString(),     // NN enqueues req. in work queue.
                Instant.ofEpochMilli(resultBeganExecutingTime).toString(),  // NN dequeues req. from work queue, begins executing it.
                Instant.ofEpochMilli(serverlessFnEndTime).toString(),       // NN returns result to client.
                Instant.ofEpochMilli(endToEndDuration).toString(),          // End-to-end duration.
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

    public boolean getIssuedViaHttp() { return issuedViaHttp; }

    public boolean getIssuedViaTcp() { return issuedViaTcp; }

    /**
     * Alias for {@code getLatency()}
     * @return The latency/end-to-end duration of this request.
     */
    public long getEndToEndDuration() {
        return getLatency();
    }

    /**
     * @return The latency/end-to-end duration of this request.
     */
    public long getLatency() {
        return resultReceivedTime - invokedAtTime;
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
//  "operation_name,request_id,client_id,origin_jvm,invoked_at_time,serverless_fn_start_time,enqueued_at_time,began_executing_time,"
//  "finished_executing_time,serverless_fn_end_time,result_received_time,invocation_duration,"
//  "preprocessing_duration,waiting_in_queue_duration,execution_duration,postprocessing_duration,return_to_client_duration,"
//  "serverless_fn_duration,end_to_end_duration,deployment_number,name_node_id,request_type,"
//  "metadata_cache_hits,metadata_cache_misses,straggler_resubmitted,num_gcs,gc_time";
        String formatString = "%-16s,%-38s,%-16s,%-22s," +                              // 4
                              "%-26s,%-26s,%-26s,%-26s,%-26s,%-26s,%-26s," +            // 7
                              "%-8s,%-8s,%-8s,%-8s,%-8s,%-8s,%-8s,%-8s," +              // 8
                              "%-3s,%-22s,%-6s,%-5s,%-5s,%-5s,%-5s,%-5s";               // 8
        writer.write(String.format(formatString,
                operationName, requestId, clientId, originJvmIdentifier,
                invokedAtTime,                    // Client invokes NN.
                serverlessFnStartTime,            // NN begins executing.
                requestEnqueuedAtTime,            // NN enqueues req. in work queue.
                resultBeganExecutingTime,         // NN dequeues req. from work queue, begins executing it.
                resultFinishedProcessingTime,     // NN finished executing the requested operation.
                serverlessFnEndTime,              // NN returns result to client.
                resultReceivedTime,               // Client receives result from NN.
                serverlessFnStartTime - invokedAtTime,                      // Invocation.
                resultBeganExecutingTime - serverlessFnStartTime,           // Pre-processing.
                0,                                                          // Waiting in queue.
                resultFinishedProcessingTime - resultBeganExecutingTime,    // Executing.
                serverlessFnEndTime - resultFinishedProcessingTime,         // Post-processing.
                resultReceivedTime - serverlessFnEndTime,                   // Returning to user.
                serverlessFunctionDuration,                                 // Total duration of the serverless func.
                endToEndDuration,                                           // End-to-end duration of the operation.
                deployment, nameNodeId, resultReceivedVia, metadataCacheHits, metadataCacheMisses,
                stragglerResubmittedToInt(), numGarbageCollections, garbageCollectionTime));
        writer.newLine();

        if (serverlessFnStartTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has serverlessFnStartTime field set to " + serverlessFnStartTime);
        else if (resultBeganExecutingTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has resultBeganExecutingTime field set to " + resultBeganExecutingTime);
        else if (resultFinishedProcessingTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has resultFinishedProcessingTime field set to " + resultFinishedProcessingTime);
        else if (serverlessFnEndTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has serverlessFnEndTime field set to " + serverlessFnEndTime);
        else if (resultReceivedTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has resultReceivedTime field set to " + resultReceivedTime);
        else if (invokedAtTime <= 0)
            System.out.println("[ERROR] OperationPerformed for request " + requestId + " has invokedAtTime field set to " + invokedAtTime);
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
            String key = entry.getKey();
            long value = entry.getValue();

            try {
                averages.put(key, (double)value / (double)counts.getOrDefault(key, 1));
            } catch (NullPointerException ex) {
                System.out.println("ERROR: NPE encountered.");
                ex.printStackTrace();
                System.out.println("Key: " + key);
            }
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
        sums.put(EXECUTION_TIME, 0L);
        sums.put(POSTPROCESSING_TIME, 0L);
        sums.put(RETURNING_TO_USER, 0L);

        for (OperationPerformed op : collection) {
            long invocationTime = sums.get(INVOCATION_TIME);
            long preprocessingTime = sums.get(PREPROCESSING_TIME);
            long executionTime = sums.get(EXECUTION_TIME);
            long postprocessingTime = sums.get(POSTPROCESSING_TIME);
            long returningToUser = sums.get(RETURNING_TO_USER);

            if (op.serverlessFnStartTime - op.invokedAtTime > 0 && op.serverlessFnStartTime - op.invokedAtTime < 1e6) {
                sums.put(INVOCATION_TIME, invocationTime + (op.serverlessFnStartTime - op.invokedAtTime));
                counts.merge(INVOCATION_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
            }
            // Now preprocessing is just everything from when request is received to when it begins executing.
            if (op.resultBeganExecutingTime - op.serverlessFnStartTime > 0) {
                sums.put(PREPROCESSING_TIME, preprocessingTime + (op.resultBeganExecutingTime - op.serverlessFnStartTime));
                counts.merge(PREPROCESSING_TIME, 1, Integer::sum); // If no value exists, put 1. Otherwise, add 1 to existing value.
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

    /**
     * Return a {@link HashMap} mapping each unique NameNode ID to the number of requests that particular NameNode serviced,
     * based on the {@link OperationPerformed} instances contained within the {@code ops} parameter.
     * @param ops Collection of {@link OperationPerformed} instances.
     * @param deploymentMapping Empty HashMap (or null). If HashMap, then mapping from NameNodeID to its deployment will be created.
     * @return {@link HashMap} that maps NameNode ID to number of requests serviced by that NameNode.
     */
    public static HashMap<Long, Integer> getRequestsPerNameNode(Collection<OperationPerformed> ops, HashMap<Long, Integer> deploymentMapping) {
        HashMap<Long, Integer> requestsPerNameNode = new HashMap<>();

        for (OperationPerformed op : ops) {
            requestsPerNameNode.merge(op.getNameNodeId(), 1, Integer::sum);

            if (deploymentMapping != null)
                deploymentMapping.computeIfAbsent(op.getNameNodeId(), nnId -> op.getDeployment());
        }

        return requestsPerNameNode;
    }

    /**
     * Return a {@link HashMap} mapping each deployment number to the number of requests that particular deployment serviced,
     * based on the {@link OperationPerformed} instances contained within the {@code ops} parameter.
     *
     * Note that if a deployment does not appear at all as the {@code deployment} field of one of the
     * {@link OperationPerformed} instances in the {@code ops} parameter, then it will not be contained within
     * the {@link HashMap} returned by this function.
     * @param ops Collection of {@link OperationPerformed} instances.
     * @return {@link HashMap} that maps deployment numbers to the number of requests serviced by that deployment.
     */
    public static HashMap<Integer, Integer> getRequestsPerDeployment(Collection<OperationPerformed> ops) {
        HashMap<Integer, Integer> requestsPerDeployment = new HashMap<>();

        for (OperationPerformed op : ops)
            requestsPerDeployment.merge(op.getDeployment(), 1, Integer::sum);

        return requestsPerDeployment;
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
