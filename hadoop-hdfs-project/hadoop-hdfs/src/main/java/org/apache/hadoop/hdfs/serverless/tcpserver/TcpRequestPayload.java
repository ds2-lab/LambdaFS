package org.apache.hadoop.hdfs.serverless.tcpserver;

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

    private String requestId;

    private String operationName;

    private boolean consistencyProtocolEnabled;

    private String serverlessFunctionLogLevel;

    private boolean benchmarkingModeEnabled;

    public TcpRequestPayload(String requestId, String operationName, boolean consistencyProtocolEnabled,
                             String serverlessFunctionLogLevel, HashMap<String, Object> fsOperationArguments,
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

    public String getServerlessFunctionLogLevel() {
        return serverlessFunctionLogLevel;
    }

    public boolean isBenchmarkingModeEnabled() { return benchmarkingModeEnabled; }

    @Override
    public String toString() {
        return String.format(
                "TcpRequestPayload(requestId=%s, operationName=%s, consistProtoEnabled=%b, logLevel=%s, numFsArgs=%d",
                requestId, operationName, consistencyProtocolEnabled, serverlessFunctionLogLevel, fsOperationArguments.size());
    }
}
