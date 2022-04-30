package org.apache.hadoop.hdfs.serverless.tcpserver;

import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Sent directly to NameNodes by clients rather than using the JSON representation.
 */
public class TcpRequestPayload implements Serializable {
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
    private transient boolean cancelled = false;

    public TcpRequestPayload(String requestId, String operationName, boolean consistencyProtocolEnabled,
                             int serverlessFunctionLogLevel, HashMap<String, Object> fsOperationArguments,
                             boolean benchmarkingModeEnabled) {
        this.requestId = requestId;
        this.operationName = operationName;
        this.consistencyProtocolEnabled = consistencyProtocolEnabled;
        this.serverlessFunctionLogLevel = serverlessFunctionLogLevel;
        this.benchmarkingModeEnabled = benchmarkingModeEnabled;

        if (fsOperationArguments != null)
            this.fsOperationArguments = fsOperationArguments;
        else
            this.fsOperationArguments = new HashMap<>();
    }

    private TcpRequestPayload() { }

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

    public boolean isCancelled() { return cancelled; }

    public void setCancelled(boolean cancelled) { this.cancelled = cancelled; }

    @Override
    public String toString() {
        return String.format(
                "TcpRequestPayload(requestId=%s, operationName=%s, consistProtoEnabled=%b, logLevel=%s, numFsArgs=%d",
                requestId, operationName, consistencyProtocolEnabled, serverlessFunctionLogLevel, fsOperationArguments.size());
    }
}
