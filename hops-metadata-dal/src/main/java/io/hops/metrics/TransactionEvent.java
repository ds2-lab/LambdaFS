package io.hops.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates metric information about a particular transaction. Includes a list of
 * {@link TransactionAttempt}, which record the timings for each attempt/retry of a
 * given transaction.
 */
public class TransactionEvent implements Serializable {
    private static final long serialVersionUID = 6838852634713013849L;
    private final List<TransactionAttempt> attempts;

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
    private final long transactionId;

    public TransactionEvent(long transactionId) {
        this.attempts = new ArrayList<>();
        this.transactionId = transactionId;
    }

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

    public void write(BufferedWriter writer) throws IOException {
        for (TransactionAttempt attempt : attempts) {
            writer.write(requestId + ",");
            attempt.write(writer);
            writer.write("," + transactionStartTime + "," + transactionEndTime + "," + transactionId + "," + success);
        }
    }
}
