package io.hops.metrics;

import java.io.Serializable;

/**
 * Encapsulates metric data for a particular attempt of a transaction.
 */
public class TransactionAttempt implements Serializable {
    private static final long serialVersionUID = 1948688753660035738L;
    /**
     * The time at which acquiring the locks and reading data from NDB began.
     */
    private long acquireLocksStart;

    /**
     * The time at which acquiring the locks and reading data from NDB finished.
     */
    private long acquireLocksEnd;

    /**
     * The time at which in-memory processing began.
     */
    private long processingStart;

    /**
     * The time at which in-memory processing finished.
     */
    private long processingEnd;

    /**
     * Time at which the consistency protocol started.
     */
    private long consistencyProtocolStart;

    /**
     * Time at which the consistency protocol finished.
     */
    private long consistencyProtocolEnd;

    /**
     * Indicates whether the consistency protocol completed successfully.
     */
    private boolean consistencyProtocolSucceeded;

    /**
     * The time at which committing the data back to NDB started.
     */
    private long commitStart;

    /**
     * The time at which committing the data back to NDB ended.
     */
    private long commitEnd;

    /**
     * Identifies which attempt/retry this transaction attempt corresponds to.
     */
    private final int attemptNumber;

    public TransactionAttempt(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    public long getAcquireLocksStart() {
        return acquireLocksStart;
    }

    public void setAcquireLocksStart(long acquireLocksStart) {
        this.acquireLocksStart = acquireLocksStart;
    }

    public long getAcquireLocksEnd() {
        return acquireLocksEnd;
    }

    public void setAcquireLocksEnd(long acquireLocksEnd) {
        this.acquireLocksEnd = acquireLocksEnd;
    }

    public long getProcessingStart() {
        return processingStart;
    }

    public void setProcessingStart(long processingStart) {
        this.processingStart = processingStart;
    }

    public long getProcessingEnd() {
        return processingEnd;
    }

    public void setProcessingEnd(long processingEnd) {
        this.processingEnd = processingEnd;
    }

    public long getCommitStart() {
        return commitStart;
    }

    public void setCommitStart(long commitStart) {
        this.commitStart = commitStart;
    }

    public long getCommitEnd() {
        return commitEnd;
    }

    public void setCommitEnd(long commitEnd) {
        this.commitEnd = commitEnd;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getConsistencyProtocolStart() {
        return consistencyProtocolStart;
    }

    public void setConsistencyProtocolStart(long consistencyProtocolStart) {
        this.consistencyProtocolStart = consistencyProtocolStart;
    }

    public long getConsistencyProtocolEnd() {
        return consistencyProtocolEnd;
    }

    public void setConsistencyProtocolEnd(long consistencyProtocolEnd) {
        this.consistencyProtocolEnd = consistencyProtocolEnd;
    }

    public boolean isConsistencyProtocolSucceeded() {
        return consistencyProtocolSucceeded;
    }

    public void setConsistencyProtocolSucceeded(boolean consistencyProtocolSucceeded) {
        this.consistencyProtocolSucceeded = consistencyProtocolSucceeded;
    }

//    @Override
//    public String toString() {
//        return "TransactionAttempt(acquireLocksStart=" + acquireLocksStart + ","
//    }
}
