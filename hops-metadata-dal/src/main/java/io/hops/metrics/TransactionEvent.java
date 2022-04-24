package io.hops.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Encapsulates metric information about a particular transaction. Includes a list of
 * {@link TransactionAttempt}, which record the timings for each attempt/retry of a
 * given transaction.
 */
public final class TransactionEvent implements Serializable {
    private static final long serialVersionUID = 6838852634713013849L;
    private List<TransactionAttempt> attempts;

    public static String LOCKING =              "LOCKING";
    public static String PROCESSING =           "IN-MEMORY PROCESSING";

    public static String CONSISTENCY_PROTOCOL = "CONSISTENCY_PROTOCOL";

    public static String INITIALIZATION =       "INITIALIZATION";
    public static String CREATE_ACKS =          "CREATE_ACKS";
    public static String SUBSCRIBE_TO_ACKS =    "SUBSCRIBE_TO_ACKS";
    public static String WRITE_ACKS_TO_NDB =    "WRITE_ACKS_NDB";
    public static String ISSUE_INVALIDATIONS =  "ISSUE_INVALIDATIONS";
    public static String WAIT_FOR_ACKS =        "WAIT_FOR_ACKS";
    public static String CLEAN_UP =             "CLEAN_UP";

    public static String COMMIT =               "COMMIT";

    /**
     * Time at which the transaction started.
     */
    private long transactionStartTime;

    /**
     * Time at which the transaction ended.
     */
    private long transactionEndTime;

    /**
     * Indicates whether the transaction was successful or not.
     */
    private boolean success;

    /**
     * Associated requestId.
     */
    private String requestId;

    /**
     * The 'operationId' instance field of the associated transaction.
     */
    private long transactionId;

    public TransactionEvent(long transactionId) {
        this.attempts = new ArrayList<>();
        this.transactionId = transactionId;
    }

    private TransactionEvent() {}

    public void addAttempt(TransactionAttempt attempt) {
        this.attempts.add(attempt);
    }

    public List<TransactionAttempt> getAttempts() {
        return attempts;
    }

    public int numberOfAttempts() { return attempts.size(); }

    public long getTransactionStartTime() {
        return transactionStartTime;
    }

    public void setTransactionStartTime(long transactionStartTime) {
        this.transactionStartTime = transactionStartTime;
    }

    public long getTransactionEndTime() {
        return transactionEndTime;
    }

    public void setTransactionEndTime(long transactionEndTime) {
        this.transactionEndTime = transactionEndTime;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TransactionEvent))
            return false;

        TransactionEvent other = (TransactionEvent)o;

        return this.requestId != null &&
                other.requestId != null &&
                this.requestId.equals(other.requestId) &&
                this.transactionId == other.transactionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result +
                ((requestId == null) ? 0 : requestId.hashCode());
        result = prime * result + Long.hashCode(transactionId);

        return result;
    }

    public static String getHeader() {
        // return "requestId," + TransactionAttempt.getHeader() + ",transactionStart,transactionEnd,transactionId,success";
        return "requestId," + TransactionAttempt.getHeader() + ",transactionStart,transactionEnd,transactionId,success";
    }

    /**
     * Get the average duration of each phase of the consistency protocol.
     */
    public static HashMap<String, Double> getAverages(Collection<TransactionEvent> collection) {
        HashMap<String, Long> sums = getSums(collection);
        HashMap<String, Double> averages = new HashMap<>();
        double n = 0.0;

        // We have to calculate `n` by counting the total number of attempts of each transaction,
        // since each individual attempt contributes to the sum.
        for (TransactionEvent ev : collection) {
            n += ev.getAttempts().size();
        }

        for (Map.Entry<String, Long> entry : sums.entrySet()) {
            averages.put(entry.getKey(), entry.getValue() / n);
        }

        return averages;
    }

    public static HashMap<String, Long> getSums(Collection<TransactionEvent> collection) {
        HashMap<String, Long> sums = new HashMap<>();
        sums.put(LOCKING, 0L);
        sums.put(PROCESSING, 0L);

        sums.put(INITIALIZATION, 0L);
        sums.put(CREATE_ACKS, 0L);
        sums.put(SUBSCRIBE_TO_ACKS, 0L);
        sums.put(WRITE_ACKS_TO_NDB, 0L);
        sums.put(ISSUE_INVALIDATIONS, 0L);
        sums.put(WAIT_FOR_ACKS, 0L);
        sums.put(CLEAN_UP, 0L);

        sums.put(COMMIT, 0L);

        for (TransactionEvent ev : collection) {
            for (TransactionAttempt attempt : ev.attempts) {
                long lockTime = sums.get(LOCKING);
                long inMemoryProcessingTime = sums.get(PROCESSING);

                long initializationTime = sums.get(INITIALIZATION);
                long createAcks = sums.get(CREATE_ACKS);
                long subscribeToAcks = sums.get(SUBSCRIBE_TO_ACKS);
                long writeAcksToNdbTime = sums.get(WRITE_ACKS_TO_NDB);
                long issueInvalidationsTime = sums.get(ISSUE_INVALIDATIONS);
                long waitForAcksTime = sums.get(WAIT_FOR_ACKS);
                long cleanUpTime = sums.get(CLEAN_UP);

                long commitTime = sums.get(COMMIT);

                sums.put(LOCKING, lockTime + (attempt.getAcquireLocksEnd() - attempt.getAcquireLocksStart()));
                sums.put(PROCESSING, inMemoryProcessingTime + (attempt.getProcessingEnd() - attempt.getProcessingStart()));

                sums.put(INITIALIZATION, initializationTime + (attempt.getConsistencyPreprocessingEnd() - attempt.getConsistencyPreprocessingStart()));
                sums.put(CREATE_ACKS, createAcks + (attempt.getConsistencyComputeAckRecordsEnd() - attempt.getConsistencyComputeAckRecordsStart()));
                sums.put(SUBSCRIBE_TO_ACKS, subscribeToAcks + (attempt.getConsistencySubscribeAckEventsEnd() - attempt.getConsistencySubscribeAckEventsStart()));
                sums.put(WRITE_ACKS_TO_NDB, writeAcksToNdbTime + (attempt.getConsistencyWriteAcksToStorageEnd() - attempt.getConsistencyWriteAcksToStorageStart()));
                sums.put(ISSUE_INVALIDATIONS, issueInvalidationsTime + (attempt.getConsistencyIssueInvalidationsEnd() - attempt.getConsistencyIssueInvalidationsStart()));
                sums.put(WAIT_FOR_ACKS, waitForAcksTime + (attempt.getConsistencyWaitForAcksEnd() - attempt.getConsistencyWaitForAcksStart()));
                sums.put(CLEAN_UP, cleanUpTime + (attempt.getConsistencyCleanUpEnd() - attempt.getConsistencyCleanUpStart()));

                sums.put(COMMIT, commitTime + (attempt.getCommitEnd() - attempt.getCommitStart()));
            }
        }

        return sums;
    }

    public static String getMetricsHeader() {
        return String.format("%-20s %-22s ", LOCKING, PROCESSING) + getConsistencyProtocolHeader() +
                String.format(" %-20s", COMMIT);
    }

    public static String getMetricsString(HashMap<String, ?> metrics) {
        return String.format("%-20s %-22s ", metrics.get(LOCKING), metrics.get(PROCESSING)) +
                getConsistencyProtocolString(metrics) + String.format(" %-20s", metrics.get(COMMIT));

    }

    public static String getConsistencyProtocolHeader() {
        return String.format("%-20s %-20s %-20s %-20s %-20s %-20s %-20s", INITIALIZATION, CREATE_ACKS,
                SUBSCRIBE_TO_ACKS, WRITE_ACKS_TO_NDB, ISSUE_INVALIDATIONS, WAIT_FOR_ACKS, CLEAN_UP);
    }

    public static String getConsistencyProtocolString(HashMap<String, ?> metrics) {
        return String.format("%-20s %-20s %-20s %-20s %-20s %-20s %-20s", metrics.get(INITIALIZATION),
                metrics.get(CREATE_ACKS), metrics.get(SUBSCRIBE_TO_ACKS), metrics.get(WRITE_ACKS_TO_NDB),
                metrics.get(ISSUE_INVALIDATIONS), metrics.get(WAIT_FOR_ACKS), metrics.get(CLEAN_UP));
    }

    public void write(BufferedWriter writer) throws IOException {
        for (TransactionAttempt attempt : attempts) {
            writer.write(requestId + ",");
            attempt.write(writer);
            writer.write("," + transactionStartTime + "," + transactionEndTime + "," + transactionId + "," + success);
            writer.newLine();
        }
    }
}
