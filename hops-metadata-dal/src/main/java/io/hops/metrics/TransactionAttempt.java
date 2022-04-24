package io.hops.metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * Encapsulates metric data for a particular attempt of a transaction.
 */
public final class TransactionAttempt implements Serializable {
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
    private int attemptNumber;

    ///////////////////////////////////////////////
    // Consistency Protocol-specific timestamps. //
    ///////////////////////////////////////////////

    /**
     * Everything that happens before the consistency protocol's formal steps begin.
     */
    private long consistencyPreprocessingStart;
    private long consistencyPreprocessingEnd;

    private long consistencyComputeAckRecordsStart;
    private long consistencyComputeAckRecordsEnd;

    private long consistencyJoinDeploymentsStart;
    private long consistencyJoinDeploymentsEnd;

    private long consistencySubscribeAckEventsStart;
    private long consistencySubscribeAckEventsEnd;

    private long consistencyWriteAcksToStorageStart;
    private long consistencyWriteAcksToStorageEnd;

    private long consistencyIssueInvalidationsStart;
    private long consistencyIssueInvalidationsEnd;

    private long consistencyEarlyUnsubscribeStart;
    private long consistencyEarlyUnsubscribeEnd;

    private long consistencyWaitForAcksStart;
    private long consistencyWaitForAcksEnd;

    private long consistencyCleanUpStart;
    private long consistencyCleanUpEnd;

    public TransactionAttempt(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    private TransactionAttempt() { }

    public void setConsistencyPreprocessingTimes(long start, long end) {
        consistencyPreprocessingStart = start;
        consistencyPreprocessingEnd = end;
    }

    public void setConsistencyComputeAckRecordTimes(long start, long end) {
        consistencyComputeAckRecordsStart = start;
        consistencyComputeAckRecordsEnd = end;
    }

    public void setConsistencyJoinDeploymentsTimes(long start, long end) {
        consistencyJoinDeploymentsStart = start;
        consistencyJoinDeploymentsEnd = end;
    }

    public void setConsistencySubscribeToAckEventsTimes(long start, long end) {
        consistencySubscribeAckEventsStart = start;
        consistencySubscribeAckEventsEnd = end;
    }

    public void setConsistencyWriteAcksToStorageTimes(long start, long end) {
        consistencyWriteAcksToStorageStart = start;
        consistencyWriteAcksToStorageEnd = end;
    }

    public void setConsistencyIssueInvalidationsTimes(long start, long end) {
        consistencyIssueInvalidationsStart = start;
        consistencyIssueInvalidationsEnd = end;
    }

    public void setConsistencyEarlyUnsubscribeTimes(long start, long end) {
        consistencyEarlyUnsubscribeStart = start;
        consistencyEarlyUnsubscribeEnd = end;
    }

    public void setConsistencyWaitForAcksTimes(long start, long end) {
        consistencyWaitForAcksStart = start;
        consistencyWaitForAcksEnd = end;
    }

    public void setConsistencyCleanUpTimes(long start, long end) {
        consistencyCleanUpStart = start;
        consistencyCleanUpEnd = end;
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

    /**
     * Write in CSV format. This does NOT write a new line.
     */
    public void write(BufferedWriter writer) throws IOException {
        writer.write(acquireLocksStart + "," + acquireLocksEnd + "," + processingStart + "," + processingEnd + "," +
                consistencyProtocolStart + "," + consistencyProtocolEnd + "," + commitStart + "," + commitEnd + "," +
                (acquireLocksEnd - acquireLocksStart) + "," + (processingEnd - processingStart) + "," +
                (consistencyProtocolEnd - consistencyProtocolStart) + "," + (commitEnd - commitStart) + "," +
                consistencyPreprocessingStart + "," + consistencyPreprocessingEnd + "," + (consistencyPreprocessingEnd - consistencyPreprocessingStart) + "," +
                consistencyComputeAckRecordsStart + "," + consistencyComputeAckRecordsEnd + "," + (consistencyComputeAckRecordsEnd - consistencyComputeAckRecordsStart) + "," +
                consistencyJoinDeploymentsStart + "," + consistencyJoinDeploymentsEnd + "," + (consistencyJoinDeploymentsEnd - consistencyJoinDeploymentsStart) + "," +
                consistencySubscribeAckEventsStart + "," + consistencySubscribeAckEventsEnd + "," + (consistencySubscribeAckEventsEnd - consistencySubscribeAckEventsStart) + "," +
                consistencyWriteAcksToStorageStart + "," + consistencyWriteAcksToStorageEnd + "," + (consistencyWriteAcksToStorageEnd - consistencyWriteAcksToStorageStart) + "," +
                consistencyIssueInvalidationsStart + "," + consistencyIssueInvalidationsEnd + "," + (consistencyIssueInvalidationsEnd - consistencyIssueInvalidationsStart) + "," +
                consistencyEarlyUnsubscribeStart + "," + consistencyEarlyUnsubscribeEnd + "," + (consistencyEarlyUnsubscribeEnd - consistencyEarlyUnsubscribeStart) + "," +
                consistencyWaitForAcksStart + "," + consistencyWaitForAcksEnd + "," + (consistencyWaitForAcksEnd - consistencyWaitForAcksStart) + "," +
                consistencyCleanUpStart + "," + consistencyCleanUpEnd + "," + (consistencyCleanUpEnd - consistencyCleanUpStart));
    }

    public static String getHeader() {
        return "acquire_locks_start,acquire_locks_end,processing_start,processing_end,consistency_protocol_start," +
                "consistency_protocol_end,commit_start,commit_end,lock_duration,processing_duration,consistency_duration,commit_duration," +
                "consistency_preprocessing_start,consistency_preprocessing_end,consistency_preprocessing_duration," +
                "consistency_compute_ack_records_start,consistency_compute_ack_records_end,consistency_compute_ack_records_start_duration," +
                "consistency_join_deployments_start,consistency_join_deployments_end,consistency_join_deployments_duration," +
                "consistency_subscribe_ack_events_start,consistency_subscribe_ack_events_end,consistency_subscribe_ack_events_duration," +
                "consistency_write_acks_start,consistency_write_acks_end,consistency_write_acks_duration," +
                "consistency_issue_invalidations_start,consistency_issue_invalidations_end,consistency_issue_invalidations_duration," +
                "consistency_early_unsubscribe_start,consistency_early_unsubscribe_end,consistency_early_unsubscribe_duration," +
                "consistency_wait_for_acks_start,consistency_wait_for_acks_end,consistency_wait_for_acks_duration," +
                "consistency_clean_up_start,consistency_clean_up_end,consistency_clean_up_duration";
    }

    @Override
    public String toString() {
        return "TransactionAttempt(acquireLocksStart=" + acquireLocksStart +
                ",acquireLocksEnd=" + acquireLocksEnd +
                ",processingStart=" + processingStart +
                ",processingEnd=" + processingEnd +
                ",consistencyProtocolStart=" + consistencyProtocolStart +
                ",consistencyProtocolEnd=" + consistencyProtocolEnd +
                ",commitStart=" + commitStart +
                ",commitEnd=" + commitEnd + ")";
    }

    public long getConsistencyPreprocessingStart() {
        return consistencyPreprocessingStart;
    }

    public long getConsistencyPreprocessingEnd() {
        return consistencyPreprocessingEnd;
    }

    public long getConsistencyComputeAckRecordsStart() {
        return consistencyComputeAckRecordsStart;
    }

    public long getConsistencyComputeAckRecordsEnd() {
        return consistencyComputeAckRecordsEnd;
    }

    public long getConsistencySubscribeAckEventsStart() {
        return consistencySubscribeAckEventsStart;
    }

    public long getConsistencySubscribeAckEventsEnd() {
        return consistencySubscribeAckEventsEnd;
    }

    public long getConsistencyWriteAcksToStorageStart() {
        return consistencyWriteAcksToStorageStart;
    }

    public long getConsistencyWriteAcksToStorageEnd() {
        return consistencyWriteAcksToStorageEnd;
    }

    public long getConsistencyIssueInvalidationsStart() {
        return consistencyIssueInvalidationsStart;
    }

    public long getConsistencyIssueInvalidationsEnd() {
        return consistencyIssueInvalidationsEnd;
    }

    public long getConsistencyEarlyUnsubscribeStart() {
        return consistencyEarlyUnsubscribeStart;
    }

    public long getConsistencyEarlyUnsubscribeEnd() {
        return consistencyEarlyUnsubscribeEnd;
    }

    public long getConsistencyWaitForAcksStart() {
        return consistencyWaitForAcksStart;
    }

    public long getConsistencyWaitForAcksEnd() {
        return consistencyWaitForAcksEnd;
    }

    public long getConsistencyCleanUpStart() {
        return consistencyCleanUpStart;
    }

    public long getConsistencyCleanUpEnd() {
        return consistencyCleanUpEnd;
    }

    public long getConsistencyJoinDeploymentsStart() {
        return consistencyJoinDeploymentsStart;
    }

    public long getConsistencyJoinDeploymentsEnd() {
        return consistencyJoinDeploymentsEnd;
    }
}
