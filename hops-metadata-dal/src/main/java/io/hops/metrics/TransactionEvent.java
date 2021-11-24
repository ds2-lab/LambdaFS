package io.hops.metrics;

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

    public TransactionEvent() {
        this.attempts = new ArrayList<>();
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
}
