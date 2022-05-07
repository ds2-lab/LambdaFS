package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import de.davidm.textplots.Histogram;
import de.davidm.textplots.Plot;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.MetaStatus;
import io.hops.metrics.TransactionEvent;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys;
import io.hops.metrics.OperationPerformed;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResult;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.NameNodeResultWithMetrics;
import org.apache.hadoop.hdfs.serverless.operation.execution.results.ServerlessFunctionMapping;
import org.apache.hadoop.hdfs.serverless.tcpserver.HopsFSUserServer;
import org.apache.hadoop.hdfs.serverless.tcpserver.TcpRequestPayload;
import org.apache.hadoop.hdfs.serverless.tcpserver.TcpTaskFuture;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExponentialBackOff;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.SERVERLESS_PLATFORM_DEFAULT;
import static org.apache.hadoop.hdfs.serverless.ServerlessNameNodeKeys.*;

/**
 * This serves as an adapter between the DFSClient interface and the serverless NameNode API.
 *
 * This basically enables the DFSClient code to remain unmodified; it just issues its commands
 * to an instance of this class, which transparently handles the serverless invoking code.
 */
public class ServerlessNameNodeClient implements ClientProtocol {

    public static final Log LOG = LogFactory.getLog(ServerlessNameNodeClient.class);

    /**
     * Responsible for invoking the Serverless NameNode(s).
     */
    public ServerlessInvokerBase<JsonObject> serverlessInvoker;

    /**
     * Issue HTTP requests to this to invoke serverless functions.
     *
     * This is the BASE endpoint, meaning we must append a number to the end of it to reach
     * an actual function. This is because functions are named as PREFIX1, PREFIX2, ..., where
     * PREFIX is user-specified/user-configured.
     */
    public String serverlessEndpointBase;

    /**
     * The name of the serverless platform being used for the Serverless NameNodes.
     */
    public String serverlessPlatformName;

    private final DFSClient dfsClient;

    private final HopsFSUserServer tcpServer;

    /**
     * Number of unique deployments.
     */
    private final int numDeployments;

    /**
     * Flag that dictates whether TCP requests can be used to perform FS operations.
     */
    private final boolean tcpEnabled;

    /**
     * Not really used by this class. Just used for debugging.
     */
    private final boolean udpEnabled;

    /**
     * Indicates whether we're being executed in a local container for testing/profiling/debugging purposes.
     */
    private final boolean localMode;

    /**
     * The log level argument to be passed to serverless functions.
     */
    protected String serverlessFunctionLogLevel;

    /**
     * Passed to serverless functions. Determines whether they execute the consistency protocol.
     */
    protected boolean consistencyProtocolEnabled = true;

    /**
     * Statistics about per-invocation latency. This includes both TCP and HTTP requests.
     */
    private final DescriptiveStatistics latency;

    /**
     * Statistics about per-invocation latency. This includes both TCP and HTTP requests.
     * This instance has a window so that we can calculate a rolling average.
     */
    private final DescriptiveStatistics latencyWithWindow;

    /**
     * Statistics about per-invocation latency. This is just for TCP requests.
     */
    private final DescriptiveStatistics latencyTcp;

    /**
     * Statistics about per-invocation latency. This is just for HTTP requests.
     */
    private final DescriptiveStatistics latencyHttp;

    /**
     * For debugging, keep track of the operations we've performed.
     */
    private final HashMap<String, OperationPerformed> operationsPerformed = new HashMap<>();

//    /**
//     * The number of operations we've issued to a NameNode.
//     * This includes both successful and failed operations.
//     * This does not count individual retries for HTTP, as those are handled by the invoker implementation.
//     */
//    private int numOperationsIssued = 0;
//
//    /**
//     * The number of operations we've issued to a NameNode via HTTP.
//     * This includes both successful and failed operations.
//     * This does not count individual retries, as those are handled by the invoker implementation.
//     */
//    private int numOperationsIssuedViaHttp = 0;
//
//    /**
//     * The number of operations we've issued to a NameNode via TCP.
//     * This includes both successful and failed operations.
//     */
//    private int numOperationsIssuedViaTcp = 0;

    /**
     * Threshold at which we stop targeting specific deployments in an effort to prevent additional pods
     * from being scheduled. This is useful when we're constraining the available vCPU to the serverless
     * cluster. This would only really be done in order to perform a "fair" comparison against a serverful
     * framework.
     *
     * When this is set to a value <= 0, the feature is disabled. Default value is -1.
     */
    private double latencyThreshold;

    /**
     * When true, we stop targeting specific deployments based on hashing parent INode IDs and instead just try
     * to reuse existing TCP connections as much as possible. This is done to hopefully prevent the serverless
     * framework from scheduling additional pods when resource availability is low.
     */
    private boolean antiThrashingModeEnabled = false;

    /**
     * We use a rolling window when computing the average latency. This is the size of that rolling window.
     */
    private int latencyWindowSize;

    /**
     * If enabled, then the client will randomly issue an HTTP request, even when TCP is available. The
     * goal in doing this is to still involve the serverless platform in the invocation/request process,
     * so that the platform can still auto-scale to some degree. There is a separate parameter that
     * controls the chance that an HTTP request is issued in place of a TCP request.
     */
    protected boolean randomHttpEnabled = false;

    /**
     * The percentage chance that a given TCP request will be replaced with an HTTP request.
     * This is only used when the {@code randomHttpEnabled} parameter is set to `true`.
     */
    protected double randomHttpChance = 0.05f;

    /**
     * When straggler mitigation is enabled, this is the factor X such that a request
     * must be delayed for (avgLatency * X) ms in order to be re-submitted.
     */
    protected int stragglerMitigationThresholdFactor;

    /**
     * When enabled, we employ a straggler mitigation technique in which requests that have been
     * submitted but not received a response for X times the average latency are resubmitted.
     */
    protected boolean stragglerMitigationEnabled;

    /**
     * Minimum length of timeout when using straggler mitigation. If it is too short, then we'll thrash (responses
     * will come back but only after we've prematurely timed out, and this will turn into a cycle). There is a
     * mechanism in-place to prevent this thrashing (the TCP server holds onto results it receives that do not have
     * an associated {@link TcpTaskFuture}, but still.
     */
    protected int minimumStragglerMitigationTimeout;

    /**
     * Turns off metric collection to save time, network transfer, and memory.
     */
    protected boolean benchmarkModeEnabled = false;

    public ServerlessNameNodeClient(Configuration conf, DFSClient dfsClient) throws IOException {
        // "https://127.0.0.1:443/api/v1/web/whisk.system/default/namenode?blocking=true";
        serverlessEndpointBase = dfsClient.serverlessEndpoint;
        serverlessPlatformName = conf.get(SERVERLESS_PLATFORM, SERVERLESS_PLATFORM_DEFAULT);
        tcpEnabled = conf.getBoolean(SERVERLESS_TCP_REQUESTS_ENABLED, SERVERLESS_TCP_REQUESTS_ENABLED_DEFAULT);
        udpEnabled = conf.getBoolean(SERVERLESS_USE_UDP, SERVERLESS_USE_UDP_DEFAULT);
        localMode = conf.getBoolean(SERVERLESS_LOCAL_MODE, SERVERLESS_LOCAL_MODE_DEFAULT);
        latencyThreshold = conf.getDouble(SERVERLESS_LATENCY_THRESHOLD, SERVERLESS_LATENCY_THRESHOLD_DEFAULT);
        latencyWindowSize = conf.getInt(SERVERLESS_LATENCY_WINDOW_SIZE, SERVERLESS_LATENCY_WINDOW_SIZE_DEFAULT);
        randomHttpEnabled = conf.getBoolean(SERVERLESS_INVOKER_RANDOM_HTTP, SERVERLESS_INVOKER_RANDOM_HTTP_DEFAULT);
        randomHttpChance = conf.getDouble(SERVERLESS_INVOKER_RANDOM_HTTP_CHANCE,
                SERVERLESS_INVOKER_RANDOM_HTTP_CHANCE_DEFAULT);
        stragglerMitigationEnabled = conf.getBoolean(SERVERLESS_STRAGGLER_MITIGATION,
                SERVERLESS_STRAGGLER_MITIGATION_DEFAULT);
        stragglerMitigationThresholdFactor = conf.getInt(SERVERLESS_STRAGGLER_MITIGATION_THRESHOLD_FACTOR,
                SERVERLESS_STRAGGLER_MITIGATION_THRESHOLD_FACTOR_DEFAULT);
        minimumStragglerMitigationTimeout = conf.getInt(SERVERLESS_STRAGGLER_MITIGATION_MIN_TIMEOUT,
                SERVERLESS_STRAGGLER_MITIGATION_MIN_TIMEOUT_DEFAULT);
        serverlessFunctionLogLevel = conf.get(
                SERVERLESS_DEFAULT_LOG_LEVEL, SERVERLESS_DEFAULT_LOG_LEVEL_DEFAULT).toUpperCase();

        if (localMode)
            numDeployments = 1;
        else
            numDeployments = conf.getInt(DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS,
                    DFSConfigKeys.SERVERLESS_MAX_DEPLOYMENTS_DEFAULT);

        LOG.info("Serverless endpoint: " + serverlessEndpointBase);
        LOG.info("Serverless platform: " + serverlessPlatformName);
        LOG.info("TCP requests are " + (tcpEnabled ? "enabled." : "disabled."));
        LOG.info("UDP requests are " + (udpEnabled ? "enabled." : "disabled."));
        LOG.debug("Default serverless log level: " + serverlessFunctionLogLevel);
        LOG.debug("Straggler mitigation is " + (stragglerMitigationEnabled ? " enabled. " +
                "Straggler mitigation factor: " + stragglerMitigationThresholdFactor + "." : "disabled."));
        LOG.debug("Latency Window: " + latencyWindowSize + ", Latency Threshold: " + latencyThreshold + " ms.");
        LOG.debug("Random HTTP " + (randomHttpEnabled ? "enabled." : "disabled.") +
                " HTTP Chance: " + randomHttpChance);

        this.serverlessInvoker = dfsClient.serverlessInvoker;

        // This should already be set to true in the DFSClient class.
        this.serverlessInvoker.setIsClientInvoker(true);

        this.dfsClient = dfsClient;

        this.tcpServer = new HopsFSUserServer(conf, this);
        this.tcpServer.startServer();

        this.latency = new DescriptiveStatistics();
        this.latencyTcp = new DescriptiveStatistics();
        this.latencyHttp = new DescriptiveStatistics();
        this.latencyWithWindow = new DescriptiveStatistics(latencyWindowSize);
    }

    public void setBenchmarkModeEnabled(boolean benchmarkModeEnabled) {
        this.benchmarkModeEnabled = benchmarkModeEnabled;
        this.serverlessInvoker.benchmarkModeEnabled = benchmarkModeEnabled;
    }

    /**
     * Extract the result from the NN.
     *
     * If the result payload is a JsonObject, then we hand things off to the base invoker class.
     *
     * @param resultPayload The payload that was returned by the NameNode to us.
     *
     * @return The result object extracted from the payload.
     */
    public Object extractResultFromNameNode(Object resultPayload) {
        if (resultPayload instanceof NameNodeResult) {
            NameNodeResult result = (NameNodeResult)resultPayload;

            ServerlessFunctionMapping functionMapping = result.getServerlessFunctionMapping();

            if (functionMapping != null) {
                serverlessInvoker.cache.addEntry(
                        functionMapping.fileOrDirectory,
                        functionMapping.parentId,
                        false);
            }

            ArrayList<Throwable> exceptions = result.getExceptions();
            if (exceptions != null && exceptions.size() > 0) {
                LOG.warn("=+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=");
                LOG.warn("The ServerlessNameNode encountered " + exceptions.size() +
                        (exceptions.size() == 1 ? " exception." : " exceptions."));

                for (Throwable t : exceptions)
                    LOG.error(t);
                LOG.warn("=+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=+==+=");
            }

            if (!benchmarkModeEnabled) {
                NameNodeResultWithMetrics nameNodeResultWithMetrics = (NameNodeResultWithMetrics)result;
                List<TransactionEvent> txEvents = nameNodeResultWithMetrics.getTxEvents();
                if (txEvents != null) {
                    this.serverlessInvoker.transactionEvents.put(nameNodeResultWithMetrics.getRequestId(), txEvents);
                }
            }

            return result.getResult();
        }
        else if (resultPayload instanceof JsonObject)
            return serverlessInvoker.extractResultFromJsonResponse((JsonObject) resultPayload);
        else
            throw new IllegalStateException("Unexpected result payload type from NN: " +
                    resultPayload.getClass().getSimpleName());
    }

    public void setConsistencyProtocolEnabled(boolean enabled) {
        this.consistencyProtocolEnabled = enabled;
        this.serverlessInvoker.setConsistencyProtocolEnabled(enabled);
    }

    public boolean getConsistencyProtocolEnabled() {
        return consistencyProtocolEnabled;
    }

    public void setServerlessFunctionLogLevel(String logLevel) {
        this.serverlessFunctionLogLevel = logLevel;
        this.serverlessInvoker.setServerlessFunctionLogLevel(logLevel);
    }

    public String getServerlessFunctionLogLevel() {
        return this.serverlessFunctionLogLevel;
    }

    public void printDebugInformation() {
        this.tcpServer.printDebugInformation();
    }

    /**
     * Used for merging latency values in from other clients into a master client that we use for book-keeping.
     * This is primarily done using Ben's HopsFS benchmarking application.
     * @param tcpLatencies Latencies from TCP requests.
     * @param httpLatencies Latencies from HTTP requests.
     */
    public void addLatencies(double[] tcpLatencies, double[] httpLatencies) {
        for (double tcpLatency : tcpLatencies) {
            latencyTcp.addValue(tcpLatency);
            latency.addValue(tcpLatency);
        }

        for (double httpLatency : httpLatencies) {
            latencyHttp.addValue(httpLatency);
            latency.addValue(httpLatency);
        }
    }

    /**
     * Used for merging latency values in from other clients into a master client that we use for book-keeping.
     * This is primarily done using Ben's HopsFS benchmarking application.
     * @param tcpLatencies Latencies from TCP requests.
     * @param httpLatencies Latencies from HTTP requests.
     */
    public void addLatencies(Collection<Double> tcpLatencies, Collection<Double> httpLatencies) {
        for (double tcpLatency : tcpLatencies) {
            latencyTcp.addValue(tcpLatency);
            latency.addValue(tcpLatency);
        }

        for (double httpLatency : httpLatencies) {
            latencyHttp.addValue(httpLatency);
            latency.addValue(httpLatency);
        }
    }

    /**
     * Add latency values to the statistics objects. Only adds the values if they are positive. So, if
     * you only want to add a tcp latency, then pass something < 0 for httpLatency.
     */
    private void addLatency(double tcpLatency, double httpLatency) {
        if (tcpLatency > 0) {
            latencyTcp.addValue(tcpLatency);
            latency.addValue(tcpLatency);
            latencyWithWindow.addValue(tcpLatency);
        }

        if (httpLatency > 0) {
            latencyHttp.addValue(httpLatency);
            latency.addValue(httpLatency);
            latencyWithWindow.addValue(httpLatency);
        }

        // If the latency threshold is <= 0, then we don't bother with this feature.
        if (latencyThreshold > 0) {
            double averageLatency = latencyWithWindow.getMean();

            // If anti-thrashing mode is already enabled, then the latency being high doesn't change anything.
            // Thus, anti-thrashing mode must currently be disabled for us to check if latency is high.
            if (!antiThrashingModeEnabled && averageLatency >= latencyThreshold) {
                LOG.warn("Rolling average latency (" + averageLatency + " ms, n=" + latencyWindowSize +
                        ") has exceeded threshold value of " + latencyThreshold + " ms. Enabling anti-thrashing mode.");
                antiThrashingModeEnabled = true;
            }
            // Likewise, if anti-thrashing mode is not enabled, then having a low latency doesn't change anything.
            // Thus, anti-thrashing mode must already be enabled for us to check if latency is low.
            else if (antiThrashingModeEnabled && averageLatency < latencyThreshold) {
                LOG.warn("Rolling average latency (" + averageLatency + " ms, n=" + latencyWindowSize +
                        ") has fallen below threshold value of " + latencyThreshold + " ms. Disabling anti-thrashing mode.");
                antiThrashingModeEnabled = false;
            }
        }
    }

    /**
     * Calculate the timeout to use for an TCP request. If straggler mitigation is enabled, then the
     * timeout is based on the average latency, with a minimum of two milliseconds. Alternatively, if
     * straggler mitigation is disabled, then we just use the HTTP timeout for our TCP timeout.
     *
     * Also, when using straggler mitigation, we don't count every timeout towards our exponential backoff.
     * We count every other request towards the exponential backoff. So, for each exponentially backed-off retry,
     * we get one retry using the straggler mitigation technique, which does not wait for very long before
     * resubmitting the task.
     *
     * @param stragglerResubmissionAlreadyOccurred If true, then we've already resubmitted this task via straggler
     *                                             mitigation, and thus we should use the standard timeout.
     * @param backoffInterval The amount of time we would sleep during exponential backoff should this request time-out.
     *                        We include the 'backoffInterval' in the requestTimeout parameter, however, so we do not
     *                        do a separate sleep. This is in-case a result comes back while we're sleeping. Might as
     *                        well sleep while waiting for the result in case it comes in during that time, rather than
     *                        wait for a bit, then "timeout", then busy-wait (Thread.sleep(backoffInterval)), during
     *                        which we'd miss the result if it arrived.
     *
     *                        We do not include the 'backoffInterval' if 'stragglerResubmissionAlreadyOccurred' is False.
     *
     * @return The timeout to use for the next TCP request.
     */
    private long calculateRequestTimeout(boolean stragglerResubmissionAlreadyOccurred, long backoffInterval) {
        long requestTimeout;
        if (stragglerMitigationEnabled && !stragglerResubmissionAlreadyOccurred) {
            // First, calculate the potential timeout using the current average latency and the threshold factor.
            long averageLatencyRoundedDown = (long)Math.floor(latencyWithWindow.getMean());
            requestTimeout = averageLatencyRoundedDown * stragglerMitigationThresholdFactor;

            // Next, clamp the request timeout to a minimum value of at least 'minimumStragglerMitigationTimeout' ms.
            // Then, if the timeout is > than the standard timeout we'd normally use, just use the standard timeout.
            requestTimeout = Math.min(Math.max(minimumStragglerMitigationTimeout, requestTimeout), serverlessInvoker.httpTimeoutMilliseconds);
        } else {
            // Just use HTTP requestTimeout.
            requestTimeout = serverlessInvoker.httpTimeoutMilliseconds + backoffInterval;
        }

        return requestTimeout;
    }

    /**
     * Issue a TCP request to the NameNode, instructing it to perform a certain operation.
     * This function will only issue a TCP request. If the TCP connection is lost mid-operation, then
     * the request is re-submitted via HTTP.
     *
     * @param operationName The name of the FS operation that the NameNode should perform.
     * @param opArguments The arguments to be passed to the specified file system operation.
     * @param targetDeployment The deployment number of the serverless NameNode deployment associated with the
     *                             target file or directory.
     * @return The response from the NameNode.
     */
    private Object issueTCPRequest(String operationName,
                                       ArgumentContainer opArguments,
                                       int targetDeployment)
            throws InterruptedException, ExecutionException, IOException {
        long opStart = System.currentTimeMillis();
        String requestId = UUID.randomUUID().toString();

        // This contains the file system operation arguments (and everything else) that will be submitted to the NN.
        TcpRequestPayload tcpRequestPayload = new TcpRequestPayload(requestId, operationName,
                consistencyProtocolEnabled, OpenWhiskHandler.getLogLevelIntFromString(serverlessFunctionLogLevel),
                opArguments.getAllArguments(), benchmarkModeEnabled);

        boolean stragglerResubmissionAlreadyOccurred = false;
        boolean wasResubmittedViaStragglerMitigation = false;

        ExponentialBackOff exponentialBackOff = new ExponentialBackOff.Builder()
                .setMaximumRetries(5)
                .setInitialIntervalMillis(1000)
                .setMaximumIntervalMillis(5000)
                .setRandomizationFactor(0.50)
                .setMultiplier(2.0)
                .build();
        long backoffInterval = exponentialBackOff.getBackOffInMillis();
        int maxRetries = exponentialBackOff.getMaximumRetries();
        while (backoffInterval >= 0) {
            long requestTimeout = calculateRequestTimeout(stragglerResubmissionAlreadyOccurred, backoffInterval);

            long localStart;
            try {
                localStart = System.currentTimeMillis();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Issuing " + (tcpServer.isUdpEnabled() ? "UDP" : "TCP") +
                            " request for operation '" + operationName + "' now. Request ID = '" +
                            requestId + "'. Attempt " + exponentialBackOff.getNumberOfRetries() + "/" + maxRetries +
                            ".");
                } else if (LOG.isTraceEnabled()) {
                    LOG.trace("Issuing " + (tcpServer.isUdpEnabled() ? "UDP" : "TCP") + " request for operation '" +
                            operationName + "' now. Request ID = '" + requestId + "'. Attempt " +
                            exponentialBackOff.getNumberOfRetries() +
                            (stragglerResubmissionAlreadyOccurred ? "*" : "") + "/" + maxRetries +
                            ". Time elapsed so far: " + (System.currentTimeMillis() - opStart) + " ms. Timeout: " +
                            requestTimeout + " ms. " + (stragglerResubmissionAlreadyOccurred ?
                            "Straggler resubmission has already occurred." :
                            "Straggler resubmission has NOT already occurred."));
                }

//                numOperationsIssuedViaTcp++;
//                numOperationsIssued++;
                Object response = tcpServer.issueTcpRequestAndWait(targetDeployment, false, requestId,
                        operationName, tcpRequestPayload, requestTimeout, !stragglerResubmissionAlreadyOccurred);

                // This only ever happens when the request is cancelled.
                if (response instanceof TcpRequestPayload) {
                    TcpRequestPayload requestPayload = (TcpRequestPayload)response;

                    if (!requestPayload.isCancelled())
                        throw new IllegalStateException("Obtained TcpRequestPayload as response from TCP request '" +
                                requestId + "', but payload is not marked as cancelled...");

                    opArguments.addPrimitive(FORCE_REDO, true);
                    // Throw the exception. This will be caught, and the request will be resubmitted via HTTP.
                    throw new IOException("The TCP future for request " + requestId + " (operation = " + operationName +
                            ") has been cancelled. Reason: " + requestPayload.getCancellationReason() + ".");
                }

                NameNodeResult result = (NameNodeResult)response;

                // If the NameNode is reporting that this FS operation was a duplicate, then we check to see if
                // we're actually still waiting on a result for the operation. If we are, then it might have been
                // lost (e.g., network connection terminated while NN sending result back to us) or something like
                // that. In that case, we resubmit the FS operation with an additional argument indicating that the
                // NN should execute the FS operation regardless of whether it is a duplicate.
                if (result.isDuplicate()) {
                    LOG.warn("Received 'DUPLICATE REQUEST' notification via TCP for request " +
                            requestId + "...");

                    if (tcpServer.isFutureActive(requestId)) {
                        LOG.error("Request " + requestId +
                                " is still active, yet we received a 'DUPLICATE REQUEST' notification for it.");
                        LOG.warn("Resubmitting request " + requestId + " with FORCE_REDO...");

                        //payload.get(FILE_SYSTEM_OP_ARGS).getAsJsonObject().addProperty(FORCE_REDO, true);
                        tcpRequestPayload.getFsOperationArguments().put(FORCE_REDO, true);

                        // We don't sleep in this case, as there wasn't a time-out exception.
                        continue;
                    } else {
                        // This generally shouldn't happen in practice...
                        LOG.warn("Apparently we are not actually waiting on a result for request " + requestId +
                                "... Not resubmitting.");
                    }
                }

                long localEnd = System.currentTimeMillis();

                addLatency(localEnd - localStart, -1);

                if (!benchmarkModeEnabled)
                    // Collect and save/record metrics.
                    createAndStoreOperationPerformed((NameNodeResultWithMetrics)result, operationName, requestId,
                            localStart, localEnd, true, false, wasResubmittedViaStragglerMitigation);

                return response;
            } catch (TimeoutException ex) {
                // If the straggler mitigation technique/protocol is enabled, then we only count this timeout as a
                // "real" timeout (i.e., one that uses the exponential backoff mechanism) if we've already re-submitted
                // the task via the straggler mitigation protocol for this retry. If we haven't already re-submitted
                // the request, then we'll do so now, and we'll only sleep for a small interval of time.
                if (stragglerMitigationEnabled) {
                    if (stragglerResubmissionAlreadyOccurred) {
                        LOG.error("Timed out while waiting for " + (tcpServer.isUdpEnabled() ? "UDP" : "TCP") +
                                " response for request " + requestId + ".");
                        LOG.error("Already submitted a straggler mitigation request. Counting this as a 'real' timeout.");
                        stragglerResubmissionAlreadyOccurred = false; // Flip this back to false.
                        // Don't continue. We need to exponentially backoff.
                    } else {
                        if (LOG.isDebugEnabled()) LOG.debug("Will resubmit request " + requestId + " shortly via straggler mitigation...");
                        stragglerResubmissionAlreadyOccurred = true; // Technically it hasn't "already" occurred yet, but still.
                        wasResubmittedViaStragglerMitigation = true;

                        // If this isn't a write operation, then make the NN redo it so that it may go faster.
                        if (!ServerlessNameNode.isWriteOperation(operationName))
                            tcpRequestPayload.getFsOperationArguments().put(FORCE_REDO, true);
                            // payload.get(FILE_SYSTEM_OP_ARGS).getAsJsonObject().addProperty(FORCE_REDO, true);
                        continue; // Use continue statement to avoid exponential backoff.
                    }
                } else {
                    LOG.error("Timed out while waiting for TCP response for request " + requestId + ".");
                }

                backoffInterval = exponentialBackOff.getBackOffInMillis();
            } catch (IOException ex) {
                // There are two reasons an IOException may occur for which we can handle things "cleanly".
                //
                // The first is when we go to issue the TCP request and find that there are actually no available
                // connections. This can occur if the TCP connection(s) were closed in between when we checked if
                // any existed and when we went to actually issue the TCP request.
                //
                // The second is when the TCP connection is closed AFTER we have issued the request, but before we
                // receive a response from the NameNode for the request.
                //
                // In either scenario, we simply fall back to HTTP.
                LOG.error("Encountered IOException while trying to issue TCP request for operation " +
                        operationName + ":", ex);
                LOG.error("Falling back to HTTP instead. Time elapsed: " + (System.currentTimeMillis() - opStart) + " ms.");

                long startTime = System.currentTimeMillis();

//                numOperationsIssued++;
//                numOperationsIssuedViaHttp++;
                // Issue the HTTP request. This function will handle retries and timeouts.
                JsonObject response = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                        operationName,
                        dfsClient.serverlessEndpoint,
                        null, // We do not have any additional/non-default arguments to pass to the NN.
                        opArguments,
                        requestId,
                        targetDeployment);

                // TODO: This might happen if all requests to a NN time out, in which case we should
                //       either handle it more cleanly and/or throw a more meaningful exception.
                if (response == null)
                    throw new IOException("Received null response from NameNode for Request " + requestId + ", op=" +
                            operationName + ". Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms.");

                long endTime = System.currentTimeMillis();
                addLatency(-1, endTime - startTime);

                if (LOG.isDebugEnabled())
                    LOG.debug("Received result from NameNode after falling back to HTTP for operation " +
                        operationName + ". Time elapsed: " + (endTime - opStart) + ".");

                if (response.has("body"))
                    response = response.get("body").getAsJsonObject();

                if (!benchmarkModeEnabled)
                    // Collect and save/record metrics.
                    createAndStoreOperationPerformed(response, operationName, requestId, startTime, endTime,
                            true, true, wasResubmittedViaStragglerMitigation);

                return response;
            }
        }

        return null;
    }

    /**
     * This is the function that is used to submit a file system operation to a NameNode. This is done either via
     * TCP directly to a NameNode in the target deployment or via an HTTP invocation through OpenWhisk (or whatever
     * serverless platform is being used).
     *
     * @param operationName The name of the FS operation that the NameNode should perform.
     * @param serverlessEndpoint The (base) OpenWhisk URI of the serverless NameNode(s).
     * @param nameNodeArguments The command-line arguments to be given to the NN, should it be created within the NN
     *                          function container (i.e., during a cold start).
     * @param opArguments The arguments to be passed to the specified file system operation.
     *
     * @return The result of executing the desired FS operation on the NameNode.
     */
    private Object submitOperationToNameNode(
            String operationName,
            String serverlessEndpoint,
            HashMap<String, Object> nameNodeArguments,
            ArgumentContainer opArguments) throws IOException, InterruptedException, ExecutionException {
        // Check if there's a source directory parameter, as this is the file or directory that could
        // potentially be mapped to a serverless function.
        Object srcArgument = opArguments.get(ServerlessNameNodeKeys.SRC);

        // If tcpEnabled is false, we don't even bother checking to see if we can issue a TCP request.
        if (tcpEnabled && srcArgument != null) {
            String sourceFileOrDirectory = (String)srcArgument;

            // Next, let's see if we have an entry in our cache for this file/directory.
            int mappedFunctionNumber = serverlessInvoker.getFunctionNumberForFileOrDirectory(sourceFileOrDirectory);

            // If there was indeed an entry, then we need to see if we have a connection to that NameNode.
            // If we do, then we'll concurrently issue a TCP request and an HTTP request to that NameNode.
            if (mappedFunctionNumber != -1 && tcpServer.connectionExists(mappedFunctionNumber)) {
                // If random HTTP is disabled, then just issue a TCP request.
                // Alternatively, if random HTTP is enabled, then we generate a random number between 0 and 1.
                // If this number is strictly greater than the `randomHttpChance` threshold, then we still issue
                // a TCP request. Otherwise, we'll fall all the way through an issue an HTTP request.
                //
                // For example, if `randomHttpChance` is 0.05, then we'd have to generate a number in the interval
                // [0.0, 0.05] to issue an HTTP request. If we generate a number in the interval (0.05, 1], then we
                // would just issue a TCP request.
                if (!randomHttpEnabled || Math.random() > randomHttpChance)
                    return issueTCPRequest(operationName, opArguments, mappedFunctionNumber);
            }
            else if (antiThrashingModeEnabled && tcpServer.getNumActiveConnections() > 0) {
                // If anti-thrashing mode is enabled, then we'll just try to use ANY available TCP connections.
                // By passing -1, we'll randomly select a TCP connection from among all active deployments.
                // Notice that we checked to make sure that there is at least one active TCP connection before
                // entering the body of this if-else statement. We wouldn't want to bother trying to issue a TCP
                // request if we already know there are no available TCP connections. That being said, if we lose
                // all TCP connections prior to issuing the request, then we'll just fall back to HTTP.
                return issueTCPRequest(operationName, opArguments, -1);
            }
            else {
                if (LOG.isDebugEnabled())
                    LOG.debug("Source file/directory " + sourceFileOrDirectory + " is mapped to serverless NameNode " + mappedFunctionNumber + ". TCP connection exists: " + tcpServer.connectionExists(mappedFunctionNumber));
            }
        }

        if (LOG.isDebugEnabled())
            LOG.debug("Issuing HTTP request only for operation " + operationName);

        String requestId = UUID.randomUUID().toString();

        Object srcObj = opArguments.get("src");
        String src = null;
        if (srcObj != null)
            src = (String)srcObj;

        int mappedFunctionNumber = (src != null) ? serverlessInvoker.cache.getFunction(src) : -1;

        long startTime = System.currentTimeMillis();

//        numOperationsIssued++;
//        numOperationsIssuedViaHttp++;
        // If there is no "source" file/directory argument, or if there was no existing mapping for the given source
        // file/directory, then we'll just use an HTTP request.
        JsonObject response = dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                operationName,
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                opArguments,
                requestId,
                mappedFunctionNumber);

        if (response == null)
            throw new IOException("Received null response from NameNode for Request " + requestId + ", op=" +
                    operationName + ". Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms.");

        long endTime = System.currentTimeMillis();

        addLatency(-1, endTime - startTime);

        if (LOG.isDebugEnabled())
            LOG.debug("Response: " + response);

        if (response.has("body"))
            response = response.get("body").getAsJsonObject();

        if (!benchmarkModeEnabled)
            createAndStoreOperationPerformed(response, operationName, requestId, startTime, endTime,
                    false, true, false);

        return response;
    }

    /**
     * Create and store an {@link OperationPerformed} object using the metrics stored in the response
     * from the serverless function.
     * @param response The (body of the) response from the serverless function.
     * @param operationName The name of the file system operation that was performed.
     * @param requestId The unique ID of the request associated with this file system operation.
     * @param startTime The local timestamp at which this operation began.
     * @param endTime The local timestamp at which this operation completed.
     * @param issuedViaTCP Indicates whether the associated operation was issued via TCP to a NN.
     * @param issuedViaHTTP Indicates whether the associated operation was issued via HTTP to a NN.
     */
    private void createAndStoreOperationPerformed(JsonObject response, String operationName, String requestId,
                                                  long startTime, long endTime, boolean issuedViaTCP,
                                                  boolean issuedViaHTTP, boolean wasResubmittedViaStragglerMitigation) {
        if (benchmarkModeEnabled)
            return;

        try {
            long nameNodeId = -1;
            if (response.has(NAME_NODE_ID))
                nameNodeId = response.get(NAME_NODE_ID).getAsLong();

            int deployment = -1;
            if (response.has(DEPLOYMENT_NUMBER))
                deployment = response.get(DEPLOYMENT_NUMBER).getAsInt();

            int cacheHits = response.get(CACHE_HITS).getAsInt();
            int cacheMisses = response.get(CACHE_MISSES).getAsInt();

            long fnStartTime = response.get(FN_START_TIME).getAsLong();
            long fnEndTime = response.get(FN_END_TIME).getAsLong();

            long enqueuedAt = response.get(ENQUEUED_TIME).getAsLong();
            long dequeuedAt = response.get(DEQUEUED_TIME).getAsLong();
            long finishedProcessingAt = response.get(PROCESSING_FINISHED_TIME).getAsLong();

            long numGarbageCollections = response.get(NUMBER_OF_GCs).getAsLong();
            long garbageCollectionTime = response.get(GC_TIME).getAsLong();

            OperationPerformed operationPerformed
                    = new OperationPerformed(operationName, requestId,
                    startTime, endTime, enqueuedAt, dequeuedAt, fnStartTime, fnEndTime,
                    deployment, issuedViaHTTP, issuedViaTCP, response.get(REQUEST_METHOD).getAsString(),
                    nameNodeId, cacheMisses, cacheHits, finishedProcessingAt, wasResubmittedViaStragglerMitigation,
                    this.dfsClient.clientName, numGarbageCollections, garbageCollectionTime);
            operationsPerformed.put(requestId, operationPerformed);
        } catch (NullPointerException ex) {
            LOG.error("Unexpected NullPointerException encountered while creating OperationPerformed from JSON response:", ex);
            LOG.error("Response: " + response);
        }
    }

    /**
     * Create and store an {@link OperationPerformed} object using the metrics stored in the response
     * from the serverless function.
     * @param result The result from the serverless NameNode.
     * @param operationName The name of the file system operation that was performed.
     * @param requestId The unique ID of the request associated with this file system operation.
     * @param startTime The local timestamp at which this operation began.
     * @param endTime The local timestamp at which this operation completed.
     * @param issuedViaTCP Indicates whether the associated operation was issued via TCP to a NN.
     * @param issuedViaHTTP Indicates whether the associated operation was issued via HTTP to a NN.
     */
    private void createAndStoreOperationPerformed(NameNodeResultWithMetrics result, String operationName,
                                                  String requestId, long startTime, long endTime, boolean issuedViaTCP,
                                                  boolean issuedViaHTTP, boolean wasResubmittedViaStragglerMitigation) {
        long nameNodeId = result.getNameNodeId();

        int deployment = result.getDeploymentNumber();

        int cacheHits = result.getMetadataCacheHits();
        int cacheMisses = result.getMetadataCacheMisses();

        long fnStartTime = result.getFnStartTime();
        long fnEndTime = result.getFnEndTime();

        long enqueuedAt = result.getEnqueuedTime();
        long dequeuedAt = result.getDequeuedTime();
        long finishedProcessingAt = result.getProcessingFinishedTime();

        long garbageCollectionTime = result.getGarbageCollectionTime();
        long numGarbageCollections = result.getNumGarbageCollections();

        OperationPerformed operationPerformed
                = new OperationPerformed(operationName, requestId,
                startTime, endTime, enqueuedAt, dequeuedAt, fnStartTime, fnEndTime,
                deployment, issuedViaHTTP, issuedViaTCP, result.getRequestMethod(),
                nameNodeId, cacheMisses, cacheHits, finishedProcessingAt, wasResubmittedViaStragglerMitigation,
                this.dfsClient.clientName, numGarbageCollections, garbageCollectionTime);
        operationsPerformed.put(requestId, operationPerformed);
    }

    /**
     * Return the operations performed by this client.
     */
    public List<OperationPerformed> getOperationsPerformed() {
        return new ArrayList<>(operationsPerformed.values());
    }

    /**
     * Clear the collection of operations performed.
     */
    public void clearOperationsPerformed() {
        this.operationsPerformed.clear();
    }

    public void addOperationPerformed(OperationPerformed operationPerformed) {
        operationsPerformed.put(operationPerformed.getRequestId(), operationPerformed);
    }

    public void addOperationPerformeds(Collection<OperationPerformed> operationPerformeds) {
        for (OperationPerformed op : operationPerformeds)
            operationsPerformed.put(op.getRequestId(), op);
    }

    public void addOperationPerformeds(OperationPerformed[] operationPerformeds) {
        for (OperationPerformed op : operationPerformeds)
            operationsPerformed.put(op.getRequestId(), op);
    }

    /**
     * Allows for dynamically changing the latency threshold at runtime.
     * @param threshold New latency threshold.
     */
    public void setLatencyThreshold(double threshold) { this.latencyThreshold = threshold; }

    public double getLatencyThreshold() { return this.latencyThreshold; }

    /**
     * Return the list of the operations we've performed. This is just used for debugging purposes.
     */
    public void printOperationsPerformed() {
        List<OperationPerformed> opsPerformedList = new ArrayList<>(operationsPerformed.values());
        Collections.sort(opsPerformedList);

        /*String[] columnNames = {
          "Op Name", "Start Time", "End Time", "Duration (ms)", "Deployment", "HTTP", "TCP"
        };*/

        System.out.println("====================== Operations Performed ======================");
        System.out.println("Number performed: " + operationsPerformed.size());
        // System.out.println(OperationPerformed.getToStringHeader());

        DescriptiveStatistics httpStatistics = new DescriptiveStatistics();
        DescriptiveStatistics tcpStatistics = new DescriptiveStatistics();
        DescriptiveStatistics resubmittedStatistics = new DescriptiveStatistics();
        DescriptiveStatistics notResubmittedStatistics = new DescriptiveStatistics();

        DescriptiveStatistics numGcStatistics = new DescriptiveStatistics();
        DescriptiveStatistics gcTimeStatistics = new DescriptiveStatistics();

        for (OperationPerformed operationPerformed : opsPerformedList) {
            if (operationPerformed.getIssuedViaHttp()) {
                double latency = operationPerformed.getLatency();
                if (latency > 0 && latency < 1e6) {
                    httpStatistics.addValue(latency);

                    if (latency >= 150)
                        LOG.warn("FOUND HTTP LATENCY OF " + latency + " MS. TASK ID: " + operationPerformed.getRequestId());
                }
            }
            if (operationPerformed.getIssuedViaTcp()) {
                double latency = operationPerformed.getLatency();
                if (latency > 0 && latency < 1e6) {
                    tcpStatistics.addValue(latency);

                    if (latency >= 150)
                        LOG.warn("FOUND TCP LATENCY OF " + latency + " MS. TASK ID: " + operationPerformed.getRequestId());
                }
            }

            if (operationPerformed.getStragglerResubmitted()) {
                resubmittedStatistics.addValue(operationPerformed.getResultReceivedTime() - operationPerformed.getInvokedAtTime());
            } else {
                notResubmittedStatistics.addValue(operationPerformed.getResultReceivedTime() - operationPerformed.getInvokedAtTime());
            }

            numGcStatistics.addValue(operationPerformed.getNumGarbageCollections());
            gcTimeStatistics.addValue(operationPerformed.getGarbageCollectionTime());
        }

        System.out.println("\n-- SUMS ----------------------------------------------------------------------------------------------------------------------");
        System.out.println(OperationPerformed.getMetricsHeader());
        System.out.println(OperationPerformed.getMetricsString(OperationPerformed.getSums(opsPerformedList)));

        System.out.println("\n-- AVERAGES ------------------------------------------------------------------------------------------------------------------");
        System.out.println(OperationPerformed.getMetricsHeader());
        System.out.println(OperationPerformed.getMetricsString(OperationPerformed.getAverages(opsPerformedList)));

        System.out.println("\n-- REQUESTS PER DEPLOYMENT ---------------------------------------------------------------------------------------------------");
        HashMap<Integer, Integer> requestsPerDeployment = OperationPerformed.getRequestsPerDeployment(opsPerformedList);
        StringBuilder deploymentHeader = new StringBuilder();
        for (int i = 0; i < numDeployments; i++)
            deploymentHeader.append(i).append('\t');
        System.out.println(deploymentHeader);
        StringBuilder valuesString = new StringBuilder();
        for (int i = 0; i < numDeployments; i++) {
            int requests = requestsPerDeployment.getOrDefault(i, 0);
            valuesString.append(requests).append("\t");
        }
        System.out.println(valuesString);

        System.out.println("\n-- REQUESTS PER NAMENODE  ----------------------------------------------------------------------------------------------------");
        HashMap<Long, Integer> deploymentMapping = new HashMap<>();
        HashMap<Long, Integer> requestsPerNameNode = OperationPerformed.getRequestsPerNameNode(opsPerformedList, deploymentMapping);
        StringBuilder formatString = new StringBuilder();
        int i = 0;
        for (Long nameNodeId : requestsPerNameNode.keySet()) {
            formatString.append("%-25s ");
            i++;

            // Line break every five lines.
            if (i % 5 == 0)
                formatString.append("\n");
        }

        String[] idsWithDeployment = new String[requestsPerNameNode.size()];
        int idx = 0;
        for (Long nameNodeId : requestsPerNameNode.keySet())
            idsWithDeployment[idx++] = nameNodeId + " (" + deploymentMapping.get(nameNodeId) + ")";

        System.out.println(String.format(formatString.toString(), idsWithDeployment));
        System.out.println(String.format(formatString.toString(), requestsPerNameNode.values().toArray()));
        System.out.println("Number of Unique NameNodes: " + requestsPerNameNode.size());

        System.out.println("\n-- Current HTTP & TCP Statistics ----------------------------------------------------------------------------------------------------");
        System.out.println("Latency TCP (ms) [min: " + tcpStatistics.getMin() + ", max: " + tcpStatistics.getMax() +
                ", avg: " + tcpStatistics.getMean() + ", std dev: " + tcpStatistics.getStandardDeviation() +
                ", N: " + tcpStatistics.getN() + "]");
        System.out.println("Latency HTTP (ms) [min: " + httpStatistics.getMin() + ", max: " + httpStatistics.getMax() +
                ", avg: " + httpStatistics.getMean() + ", std dev: " + httpStatistics.getStandardDeviation() +
                ", N: " + httpStatistics.getN() + "]");

        try {
            printHistograms(httpStatistics, tcpStatistics);
        } catch (NotStrictlyPositiveException ex) {
            LOG.error("Encountered 'NotStrictlyPositiveException' while trying to generate latency histograms.");
        }

        System.out.println("\n-- Garbage Collection Statistics -----------------------------------------------------------------------------------------------------");
        System.out.println("Total number of GCs: " + numGcStatistics.getSum());
        System.out.println("Total time spent GC-ing: " + gcTimeStatistics.getSum() + " ms");
        System.out.println("Average number of GCs per task: " + numGcStatistics.getMean());
        System.out.println("Average time spent GC-ing per task: " + gcTimeStatistics.getMean() + " ms");
        System.out.println("Largest number of GCs for a single task: " + numGcStatistics.getMax());
        System.out.println("Longest time spent GC-ing for a single task: " + gcTimeStatistics.getMax() + " ms");

        System.out.println("\n-- Lifetime HTTP & TCP Statistics ----------------------------------------------------------------------------------------------------");
        printLatencyStatisticsDetailed(0);

        if (stragglerMitigationEnabled) {
            System.out.println("\n-- Straggler Mitigation Statistics --------------------------------------------------------------------------------------------------");
            System.out.println("Resubmitted [min: " + resubmittedStatistics.getMin() + ", max: " + resubmittedStatistics.getMax() +
                    ", avg: " + resubmittedStatistics.getMean() + ", std dev: " + resubmittedStatistics.getStandardDeviation() +
                    ", N: " + resubmittedStatistics.getN() + "]");
            System.out.println("NOT Resubmitted [min: " + notResubmittedStatistics.getMin() + ", max: " + notResubmittedStatistics.getMax() +
                    ", avg: " + notResubmittedStatistics.getMean() + ", std dev: " + notResubmittedStatistics.getStandardDeviation() +
                    ", N: " + notResubmittedStatistics.getN() + "]");

        }
        System.out.println("\n==================================================================");
    }

    /**
     * Print histograms for the latency distributions for both TCP and HTTP requests.
     */
    private void printHistograms(DescriptiveStatistics httpStatistics, DescriptiveStatistics tcpStatistics) {
        if (httpStatistics != null && httpStatistics.getN() > 1) {
            // Calculating bin width: https://stats.stackexchange.com/questions/798/calculating-optimal-number-of-bins-in-a-histogram
            double binWidthHttp = 2 * httpStatistics.getPercentile(0.75) - httpStatistics.getPercentile(0.25) * Math.pow(httpStatistics.getN(), -1.0 / 3.0);
            int numBinsHttp = (int) ((httpStatistics.getMax() - httpStatistics.getMin()) / binWidthHttp);
            numBinsHttp = Math.min(numBinsHttp, 100);
            Plot currentHttpPlot = new Histogram.HistogramBuilder(
                    Pair.create("HTTP Latencies", httpStatistics.getValues()))
                    .setBinNumber(numBinsHttp)
                    .plotObject();
            System.out.println("\nHistogram of HTTP Latencies:");
            currentHttpPlot.printPlot(true);
        }

        if (tcpStatistics != null && tcpStatistics.getN() > 1) {
            double binWidthTcp = 2 * tcpStatistics.getPercentile(0.75) - tcpStatistics.getPercentile(0.25) * Math.pow(tcpStatistics.getN(), -1.0 / 3.0);
            int numBinsTcp = (int) ((tcpStatistics.getMax() - tcpStatistics.getMin()) / binWidthTcp);
            numBinsTcp = Math.min(numBinsTcp, 100);
            Plot currentTcpPlot = new Histogram.HistogramBuilder(
                    Pair.create("TCP Latencies", tcpStatistics.getValues()))
                    .setBinNumber(numBinsTcp)
                    .plotObject();
            System.out.println("Histogram of TCP Latencies:");
            currentTcpPlot.printPlot(true);
        }
    }

    /**
     * Print the average latency.
     *
     * If choice <= 0, prints both TCP and HTTP.
     * If choice == 1, prints just TCP.
     * If choice > 1, prints just HTTP.
     * @param choice If choice <= 0, prints both TCP and HTTP. If choice == 1, prints just TCP. If
     *               choice > 1, prints just HTTP.
     */
    public void printLatencyStatistics(int choice) {
        if (choice <= 0) {
            System.out.println("AVG Latency: Both: " + latency.getMean() + " ms, TCP: " + latencyTcp.getMean() +
                    "ms, HTTP: " + latencyHttp.getMean() + " ms");
        } else if (choice == 1) {
            System.out.println("AVG Latency (TCP): " + latencyTcp.getMean());
        } else {
            System.out.println("AVG Latency (HTTP): " + latencyHttp.getMean());
        }
    }

    /**
     * Print the average, min, max, std. dev of latency.
     *
     * If choice <= 0, prints both TCP and HTTP.
     * If choice == 1, prints just TCP.
     * If choice > 2, prints just HTTP.
     * @param choice If choice <= 0, prints both TCP and HTTP. If choice == 1, prints just TCP. If
     *               choice > 1, prints just HTTP.
     */
    public void printLatencyStatisticsDetailed(int choice) {
        if (choice <= 0) {
            System.out.println("AVG Latency (ms): Both: " + latency.getMean() + ", TCP: " + latencyTcp.getMean() +
                    ", HTTP: " + latencyHttp.getMean() + " ");
            System.out.println("Min Latency (ms): Both: " + latency.getMin() + ", TCP: " + latencyTcp.getMin() +
                    ", HTTP: " + latencyHttp.getMin() + " ");
            System.out.println("Max Latency (ms): Both: " + latency.getMax() + ", TCP: " + latencyTcp.getMax() +
                    ", HTTP: " + latencyHttp.getMax() + "");
            System.out.println("STD DEV Latency: Both: " + latency.getStandardDeviation() + ", TCP: " +
                    latencyTcp.getStandardDeviation() + ", HTTP: " + latencyHttp.getStandardDeviation() + "");
        } else if (choice == 1) {
            LOG.info("AVG Latency (TCP): " + latencyTcp.getMean());
            LOG.info("MIN Latency (TCP): " + latencyTcp.getMin());
            LOG.info("MAX Latency (TCP): " + latencyTcp.getMax());
            LOG.info("STD DEV Latency (TCP): " + latencyTcp.getStandardDeviation());
        } else {
            LOG.info("AVG Latency (HTTP): " + latencyHttp.getMean());
            LOG.info("MIN Latency (HTTP): " + latencyHttp.getMin());
            LOG.info("MAX Latency (HTTP): " + latencyHttp.getMax());
            LOG.info("STD DEV Latency (HTTP): " + latencyHttp.getStandardDeviation());
        }
    }

    /**
     * Return a copy of the latency (TCP & HTTP) DescriptiveStatistics object.
     */
    public DescriptiveStatistics getLatencyStatistics() {
        return latency.copy();
    }

    /**
     * Return a copy of the latency (TCP & HTTP) DescriptiveStatistics object.
     */
    public DescriptiveStatistics getLatencyHttpStatistics() {
        return latencyHttp.copy();
    }

    /**
     * Return a copy of the latency (TCP & HTTP) DescriptiveStatistics object.
     */
    public DescriptiveStatistics getLatencyTcpStatistics() {
        return latencyTcp.copy();
    }

    /**
     * Return a copy of the moving window latency (TCP & HTTP) DescriptiveStatistics object.
     */
    public DescriptiveStatistics getLatencyWithWindowStatistics() { return this.latencyWithWindow.copy(); }

    /**
     * Clear both HTTP and TCP latency values.
     */
    public void clearLatencyValues() {
        this.latency.clear();
        this.latencyHttp.clear();
        this.latencyTcp.clear();
        this.latencyWithWindow.clear();
    }

    /**
     * Clear HTTP latency values.
     */
    public void clearLatencyValuesHttp() { this.latencyHttp.clear();}

    /**
     * Clear TCP latency values.
     */
    public void clearLatencyValuesTcp() { this.latencyTcp.clear();}

    /**
     * Shuts down this client. Currently, the only steps taken during shut-down is the stopping of the TCP server.
     */
    public void stop() {
        if (LOG.isDebugEnabled())
            LOG.debug("ServerlessNameNodeClient stopping now...");
        this.tcpServer.stop();
    }

    @Override
    public JsonObject latencyBenchmark(String connectionUrl, String dataSource, String query, int id) throws SQLException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
        LocatedBlocks locatedBlocks = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("offset", offset);
        opArguments.put("length", length);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getBlockLocations",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getBlockLocations to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getBlockLocations to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            locatedBlocks = (LocatedBlocks)result;

        return locatedBlocks;
    }

    @Override
    public LocatedBlocks getMissingBlockLocations(String filePath) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addBlockChecksum(String src, int blockIndex, long checksum) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long getBlockChecksum(String src, int blockIndex) throws IOException {
        return 0;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        FsServerDefaults serverDefaults = null;

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getServerDefaults",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    new ArgumentContainer());
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getServerDefaults to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getServerDefaults to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            serverDefaults = (FsServerDefaults)result;

        return serverDefaults;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions, EncodingPolicy policy)
            throws IOException {
        // We need to pass a series of arguments to the Serverless NameNode. We prepare these arguments here
        // in a HashMap and pass them off to the ServerlessInvoker, which will package them up in the required
        // format for the Serverless NameNode.
        HdfsFileStatus stat = null;

        // Arguments for the 'create' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("masked", masked.toShort());
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, dfsClient.clientName);

        // Convert this argument (to the 'create' function) to a String so that we can send it over JSON.
        DataOutputBuffer out = new DataOutputBuffer();
        ObjectWritable.writeObject(out, flag, flag.getClass(), null);
        byte[] objectBytes = out.getData();
        String enumSetBase64 = Base64.encodeBase64String(objectBytes);

        opArguments.put("enumSetBase64", enumSetBase64);
        opArguments.put("createParent", createParent);
        LOG.warn("Using hard-coded replication value of 1.");
        opArguments.put("replication", (short)1);
        opArguments.put("blockSize", blockSize);

        // Include a flag to indicate whether the policy is non-null.
        opArguments.put("policyExists", policy != null);

        // Only include these if the policy is non-null.
        if (policy != null) {
            opArguments.put("codec", policy.getCodec());
            opArguments.put("targetReplication", policy.getTargetReplication());
        }

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "create",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation create to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation create to NameNode.");
        }

        // Extract the result from the Json response.
        // If there's an exception, then it will be logged by this function.
        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            stat = (HdfsFileStatus)result;

        return stat;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions)
            throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException,
            FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException,
            SafeModeException, UnresolvedLinkException, IOException {
        return this.create(src, masked, clientName, flag, createParent, replication, blockSize, supportedVersions, null);
    }

    @Override
    public LastBlockWithStatus append(String src, String clientName, EnumSetWritable<CreateFlag> flag) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        LastBlockWithStatus stat = null;

        // Arguments for the 'append' filesystem operation.
        ArgumentContainer opArguments = new ArgumentContainer();

        // Serialize the `EnumSetWritable<CreateFlag> flag` argument.
        DataOutputBuffer out = new DataOutputBuffer();
        ObjectWritable.writeObject(out, flag, flag.getClass(), null);
        byte[] objectBytes = out.getData();
        String enumSetBase64 = Base64.encodeBase64String(objectBytes);

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put(ServerlessNameNodeKeys.FLAG, enumSetBase64);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "append",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation append to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation append to NameNode.");
        }

        // Extract the result from the Json response.
        // If there's an exception, then it will be logged by this function.
        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            stat = (LastBlockWithStatus)result;

        return stat;
    }

    @Override
    public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(byte storagePolicyID) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return new BlockStoragePolicy[0];
    }

    @Override
    public void setStoragePolicy(String src, String policyName) throws UnresolvedLinkException, FileNotFoundException, QuotaExceededException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setMetaStatus(String src, MetaStatus metaStatus) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("metaStatus", metaStatus.ordinal());

        try {
            submitOperationToNameNode(
                    "setMetaStatus",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setMetaStatus to NameNode:", ex);
        }
    }

    @Override
    public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("permission", permission);

        try {
            submitOperationToNameNode(
                    "setPermission",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setPermission to NameNode:", ex);
        }
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("username", username);
        opArguments.put("groupname", groupname);

        try {
            submitOperationToNameNode(
                    "setOwner",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation setOwner to NameNode:", ex);
        }
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("holder", holder);
        opArguments.put("fileId", fileId);
        opArguments.put("b", b);

        try {
            submitOperationToNameNode(
                    "abandonBlock",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation abandonBlock to NameNode:", ex);
        }
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes,
                                 long fileId, String[] favoredNodes) throws IOException, ClassNotFoundException {
        // HashMap<String, Object> opArguments = new HashMap<>();
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("previous", previous);
        opArguments.put("fileId", fileId);
        opArguments.put("favoredNodes", favoredNodes);
        opArguments.put("excludeNodes", excludeNodes);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "addBlock",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addBlock to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addBlock to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);

        if (result != null) {
            LocatedBlock locatedBlock = (LocatedBlock) result;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Result returned from addBlock() is of type: " + result.getClass().getSimpleName());
                LOG.debug("LocatedBlock returned by addBlock(): " + locatedBlock);
            }

            return locatedBlock;
        }

        return null;
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src, long fileId, ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId, byte[] data) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("last", last);
        opArguments.put("fileId", fileId);
        opArguments.put("data", data);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "complete",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation complete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation complete to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (boolean)result;

        return true;
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public boolean rename(String src, String dst) throws UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[1];

        optionsArr[0] = 0; // 0 is the Options.Rename ordinal/value for `NONE`

        opArguments.put("options", optionsArr);

        try {
            submitOperationToNameNode(
                    "rename",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation rename to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation rename to NameNode.");
        }

        return true;
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("trg", trg);
        opArguments.put("srcs", srcs);

        try {
            submitOperationToNameNode(
                    "concat",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation concat to NameNode:", ex);
        }
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("dst", dst);

        Integer[] optionsArr = new Integer[options.length];

        for (int i = 0; i < options.length; i++) {
            optionsArr[i] = options[i].ordinal();
        }

        opArguments.put("options", optionsArr);

        try {
            submitOperationToNameNode(
                    "rename", // Not rename2, we just map 'rename' to 'renameTo' or 'rename2' or whatever
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation rename to NameNode:", ex);
        }
    }

    @Override
    public boolean truncate(String src, long newLength, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("newLength", newLength);
        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "truncate",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation truncate to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation truncate to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (boolean)result;

        return true;
    }

    @Override
    public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("recursive", recursive);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "delete",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation delete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation delete to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (boolean)result;

        return false;
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("masked", masked);
        opArguments.put("createParent", createParent);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "mkdirs",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation mkdirs to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation mkdirs to NameNode.");
        }

        Object res = extractResultFromNameNode(responseFromNN);
        if (res != null)
            return (boolean)res;

        throw new IOException("Received null response for mkdirs operation...");
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);
        opArguments.put("startAfter", startAfter);
        opArguments.put("needLocation", needLocation);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getListing",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getListing to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getListing to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (DirectoryListing)result;

        return null;
    }

    @Override
    public void renewLease(String clientName) throws AccessControlException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.addPrimitive("clientName", clientName);

        try {
            submitOperationToNameNode(
                    "renewLease",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getListing to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getListing to NameNode.");
        }
    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        return false;
    }

    @Override
    public long[] getStats() throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getStats",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getListing to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getListing to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (long[])result;

        return null;
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put("type", type.ordinal());

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getDatanodeReport",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getListing to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getListing to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (DatanodeInfo[])result;

        return null;
    }

    @Override
    public DatanodeStorageReport[] getDatanodeStorageReport(HdfsConstants.DatanodeReportType type) throws IOException {
        return new DatanodeStorageReport[0];
    }

    @Override
    public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {
        return 0;
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        return false;
    }

    @Override
    public void refreshNodes() throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getFileInfo",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getFileInfo to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation getFileInfo to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (HdfsFileStatus)result;

        return null;
    }

    @Override
    public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "isFileClosed",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation isFileClosed to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation isFileClosed to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (boolean)result;

        return false;
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.SRC, src);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getFileLinkInfo",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation getFileLinkInfo to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation isFileClosed to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (HdfsFileStatus)result;

        return null;
    }

    @Override
    public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setQuota(String path, long namespaceQuota, long storagespaceQuota, StorageType type) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

    }

    @Override
    public void fsync(String src, long inodeId, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("block", block);

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "updateBlockForPipeline",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation complete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation complete to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (LocatedBlock)result;

        return null;
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock,
                               DatanodeID[] newNodes, String[] newStorages) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        opArguments.put(ServerlessNameNodeKeys.CLIENT_NAME, clientName);
        opArguments.put("oldBlock", oldBlock);
        opArguments.put("newBlock", newBlock);
        opArguments.put("newNodes", newNodes);
        opArguments.put("newStorages", newStorages);

        try {
            submitOperationToNameNode(
                    "updatePipeline",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation complete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation complete to NameNode.");
        }
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        return 0;
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void ping() throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void prewarm(int numPingsPerThread, int numThreadsPerDeployment) throws IOException {
        Thread[] threads = new Thread[numDeployments * numThreadsPerDeployment];
        CountDownLatch startLatch = new CountDownLatch(numDeployments * numThreadsPerDeployment);

        int counter = 0;
        for (int deploymentNumber = 0; deploymentNumber < numDeployments; deploymentNumber++) {
            final int depNum = deploymentNumber;
            for (int j = 0; j < numThreadsPerDeployment; j++) {
                Thread thread = new Thread(() -> {
                    if (LOG.isDebugEnabled())
                        LOG.debug("Invoking deployment " + depNum + " a total of " + numPingsPerThread + "x.");
                    startLatch.countDown();
                    for (int i = 0; i < numPingsPerThread; i++) {
                        String requestId = UUID.randomUUID().toString();

                        // If there is no "source" file/directory argument, or if there was no existing mapping for the given source
                        // file/directory, then we'll just use an HTTP request.
                        try {
                            dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                                    "prewarm",
                                    dfsClient.serverlessEndpoint,
                                    null, // We do not have any additional/non-default arguments to pass to the NN.
                                    new ArgumentContainer(),
                                    requestId,
                                    depNum);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                threads[counter++] = thread;
            }
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread: threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void ping(int targetDeployment) throws IOException {
        String requestId = UUID.randomUUID().toString();

        // If there is no "source" file/directory argument, or if there was no existing mapping for the given source
        // file/directory, then we'll just use an HTTP request.
        dfsClient.serverlessInvoker.invokeNameNodeViaHttpPost(
                "ping",
                dfsClient.serverlessEndpoint,
                null, // We do not have any additional/non-default arguments to pass to the NN.
                new ArgumentContainer(),
                requestId,
                targetDeployment);
    }

    @Override
    public SortedActiveNodeList getActiveNamenodesForClient() throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();

        Object responseFromNN;
        try {
            responseFromNN = submitOperationToNameNode(
                    "getActiveNamenodesForClient",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation complete to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation complete to NameNode.");
        }

        Object result = extractResultFromNameNode(responseFromNN);
        if (result != null)
            return (SortedActiveNodeList)result;

        return null;
    }

    @Override
    public void changeConf(List<String> props, List<String> newVals) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public EncodingStatus getEncodingStatus(String filePath) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void encodeFile(String filePath, EncodingPolicy policy) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void revokeEncoding(String filePath, short replication) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LocatedBlock getRepairedBlockLocations(String sourcePath, String parityPath, LocatedBlock block, boolean isParity) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void checkAccess(String path, FsAction mode) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public LastUpdatedContentSummary getLastUpdatedContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeDefaultAcl(String src) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeAcl(String src) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public AclStatus getAclStatus(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void createEncryptionZone(String src, String keyName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public EncryptionZone getEZForPath(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public List<XAttr> listXAttrs(String src) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeXAttr(String src, XAttr xAttr) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        return 0;
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId, CacheDirectiveInfo filter) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void modifyCachePool(CachePoolInfo req) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void removeCachePool(String pool) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool) throws IOException {
        throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void addUser(String userName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);

        try {
            submitOperationToNameNode(
                    "addUser",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addUser to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addUser to NameNode.");
        }
    }

    @Override
    public void addGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "addGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addGroup to NameNode.");
        }
    }

    @Override
    public void addUserToGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "addUserToGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation addUserToGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation addUserToGroup to NameNode.");
        }
    }

    @Override
    public void removeUser(String userName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);

        try {
            submitOperationToNameNode(
                    "removeUser",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeUser to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeUser to NameNode.");
        }
    }

    @Override
    public void removeGroup(String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "removeGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeGroup to NameNode.");
        }
    }

    @Override
    public void removeUserFromGroup(String userName, String groupName) throws IOException {
        ArgumentContainer opArguments = new ArgumentContainer();
        opArguments.put("userName", userName);
        opArguments.put("groupName", groupName);

        try {
            submitOperationToNameNode(
                    "removeUserFromGroup",
                    dfsClient.serverlessEndpoint,
                    null, // We do not have any additional/non-default arguments to pass to the NN.
                    opArguments);
        } catch (ExecutionException | InterruptedException ex) {
            LOG.error("Exception encountered while submitting operation removeUserFromGroup to NameNode:", ex);
            throw new IOException("Exception encountered while submitting operation removeUserFromGroup to NameNode.");
        }
    }

    @Override
    public void invCachesUserRemoved(String userName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesGroupRemoved(String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserRemovedFromGroup(String userName, String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public void invCachesUserAddedToGroup(String userName, String groupName) throws IOException {
		throw new UnsupportedOperationException("Function has not yet been implemented.");
    }

    @Override
    public long getEpochMS() throws IOException {
        return 0;
    }

    /**
     * IMPORTANT: This function just calls setTcpServerPort() on the ServerlessInvoker instance.
     */
    public void setTcpServerPort(int tcpServerPort) {
        this.serverlessInvoker.setTcpPort(tcpServerPort);
    }

    /**
     * IMPORTANT: This function just calls setUdpServerPort() on the ServerlessInvoker instance.
     */
    public void setUdpServerPort(int udpServerPort) {
        this.serverlessInvoker.setUdpPort(udpServerPort);
    }
}
