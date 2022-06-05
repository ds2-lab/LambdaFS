package org.apache.hadoop.hdfs.serverless.userserver;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Sent directly to NameNodes by clients rather than using the JSON representation.
 */
public class TcpUdpRequestPayload implements Serializable {
    private static final long serialVersionUID = -7628398756895387675L;

    /**
     * The arguments to the file system operation.
     */
    private HashMap<String, Object> fsOperationArguments;

    /**
     * Unique ID of the request/task associated with this instance.
     */
    private String requestId;

    /**
     * The name of the FS operation to be performed.
     */
    private String operationName;

    /**
     * Controls whether the NN performs the consistency protocol or not.
     */
    private boolean consistencyProtocolEnabled;

    /**
     * Sets the log4j log-level in the NameNode.
     */
    private int serverlessFunctionLogLevel;

    /**
     * Controls whether the NameNode collects metric data or not.
     */
    private boolean benchmarkingModeEnabled;

    /**
     * Indicates whether the operation has been cancelled.
     *
     * This is transient because it is only used client-side and thus does not need to be serialized.
     */
    // private transient boolean cancelled = false;

    /**
     * Used when cancelling this task. Indicates whether the task should be re-submitted to the NN.
     *
     * This is transient because it is only used client-side and thus does not need to be serialized.
     */
    // private transient boolean shouldRetry = false;

    /**
     * Reason that this request has been cancelled. Only used if cancelled is set to true (i.e., when the
     * request gets cancelled).
     *
     * This is transient because it is only used client-side and thus does not need to be serialized.
     */
    // private transient String cancellationReason = null;

    /**
     * All the actively-used TCP ports on our VM. The NN uses these to connect to the other servers.
     */
    private List<Integer> activeTcpPorts;

    /**
     * All the actively-used TCP ports on our VM. The NN uses these to connect to the other servers.
     */
    private List<Integer> activeUdpPorts;

    public TcpUdpRequestPayload(String requestId, String operationName, boolean consistencyProtocolEnabled,
                                int serverlessFunctionLogLevel, HashMap<String, Object> fsOperationArguments,
                                boolean benchmarkingModeEnabled, List<Integer> activeTcpPorts,
                                List<Integer> activeUdpPorts) {
        this.requestId = requestId;
        this.operationName = operationName;
        this.consistencyProtocolEnabled = consistencyProtocolEnabled;
        this.serverlessFunctionLogLevel = serverlessFunctionLogLevel;
        this.benchmarkingModeEnabled = benchmarkingModeEnabled;
        this.activeTcpPorts = activeTcpPorts;
        this.activeUdpPorts = activeUdpPorts;

        if (fsOperationArguments != null)
            this.fsOperationArguments = fsOperationArguments;
        else
            this.fsOperationArguments = new HashMap<>();
    }

    private TcpUdpRequestPayload() { }

    public HashMap<String, Object> getFsOperationArguments() { return this.fsOperationArguments; }

    public String getRequestId() {
        return requestId;
    }

    public String getOperationName() {
        return operationName;
    }

    public boolean isConsistencyProtocolEnabled() {
        return consistencyProtocolEnabled;
    }

    public int getServerlessFunctionLogLevel() {
        return serverlessFunctionLogLevel;
    }

    public boolean isBenchmarkingModeEnabled() { return benchmarkingModeEnabled; }

    // public boolean isCancelled() { return cancelled; }

    // public boolean shouldRetry() { return shouldRetry; }

    public List<Integer> getActiveTcpPorts() { return this.activeTcpPorts; }

    public List<Integer> getActiveUdpPorts() { return this.activeUdpPorts; }

    // public String getCancellationReason() { return cancellationReason; }

    /**
     * Used to mark this request as cancelled. Used when the TCP connection via which this payload was submitted
     * is disconnected before a result is received.
     */
    // public void setCancelled(boolean cancelled) { this.cancelled = cancelled; }

    // public void setShouldRetry(boolean shouldRetry) { this.shouldRetry = shouldRetry; }

    // public void setCancellationReason(String cancellationReason) { this.cancellationReason = cancellationReason; }

    @Override
    public String toString() {
        return String.format(
                "TcpRequestPayload(requestId=%s, operationName=%s, consistProtoEnabled=%b, logLevel=%s, numFsArgs=%d)",
                requestId, operationName, consistencyProtocolEnabled, serverlessFunctionLogLevel, fsOperationArguments.size());
    }
}
