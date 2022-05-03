package org.apache.hadoop.hdfs.serverless;

/**
 * Global constants used as keys in HashMaps, JSON payloads, etc. by the ServerlessNameNode.
 */
public class ServerlessNameNodeKeys {
    public static final String DUPLICATE_REQUEST = "DUPLICATE-REQUEST";
    public static final String OPERATION = "op";
    public static final String CLIENT_NAME = "clientName";
    public static final String IS_CLIENT_INVOKER = "isClientInvoker";
    public static final String INVOKER_IDENTITY = "INVOKER_IDENTITY";
    public static final String FILE_SYSTEM_OP_ARGS = "fsArgs";
    public static final String RESULT = "RESULT";
    public static final String EXCEPTIONS = "EXCEPTIONS";
    public static final String REQUEST_ID = "requestId";
    public static final String SRC = "src";
    public static final String VALUE = "value";
    public static final String FUNCTION_NAME = "functionName";
    public static final String NAME_NODE_ID = "NAME_NODE_ID";
    public static final String PERFORMING_WRITE = "PERFORMING_WRITE";
    public static final String REDIRECTED_WRITE = "REDIRECTED_WRITE";
    public static final String FLAG = "flag";

    public static final String BENCHMARK_MODE = "BENCHMARK_MODE";

    public static final String ACTION_MEMORY = "actionMemory";
    public static final String REQUEST_METHOD = "requestMethod";
    public static final String CANCELLED = "cancelled";
    public static final String REASON = "reason";
    public static final String SHOULD_RETRY = "shouldRetry";
    public static final String REASON_CONNECTION_LOST = "CONNECTION_LOST";
    public static final String OPENWHISK_ACTIVATION_ID = "OPENWHISK_ACTIVATION_ID";
    public static final String OPENWHISK = "openwhisk";

    public static final String COLD_START = "COLD_START";

    public static final String STATISTICS_PACKAGE = "STATISTICS_PACKAGE";
    public static final String TRANSACTION_EVENTS = "TRANSACTION_EVENTS";

    public static final String CACHE_HITS = "CACHE_HITS";
    public static final String CACHE_MISSES = "CACHE_MISSES";

    public static final String FN_START_TIME = "FN_START_TIME";
    public static final String FN_END_TIME = "FN_END_TIME";

    public static final String ENQUEUED_TIME = "ENQUEUED_TIME";
    public static final String DEQUEUED_TIME = "DEQUEUED_TIME";

    public static final String NUMBER_OF_GCs =  "NUM_GCs";
    public static final String GC_TIME =        "GC_TIME";

    public static final String PROCESSING_FINISHED_TIME = "PROCESSING_FINISHED_TIME";

    /**
     * Used to indicate to the NN that it should re-attempt this operation, even if
     * it was already completed, as the TCP connection was dropped before the result
     * could be delivered to the user.
     */
    public static final String FORCE_REDO = "forceRedo";

    public static final String COMMAND_LINE_ARGS = "command-line-arguments";
    public static final String DEBUG_NDB = "debugNdb";
    public static final String DEBUG_STRING_NDB = "debugStringNdb";
    public static final String TCP_PORT = "TCP_PORT";
    public static final String UDP_PORT = "UDP_PORT";
    public static final String CLIENT_INTERNAL_IP = "clientInternalIp";
    public static final String TCP_ENABLED = "tcpEnabled";
    public static final String UDP_ENABLED = "udpEnabled";
    public static final String LOG_LEVEL = "LOG_LEVEL";
    public static final String CONSISTENCY_PROTOCOL_ENABLED = "CONSISTENCY_PROTOCOL_ENABLED";

    public static final String LOCAL_MODE = "LOCAL_MODE";
    public static final String DEPLOYMENT_NUMBER = "DEPLOYMENT_NUMBER";
    public static final String DEPLOYMENT_MAPPING = "DEPLOYMENT_MAPPING";
    public static final String FILE_OR_DIR = "fileOrDirectory";
    public static final String PARENT_ID = "parentId";
    public static final String FUNCTION = "function";

    public static final String LOCAL_MODE_ENDPOINT = "http://localhost:8080/run";
    public static final String LOCAL_MODE_ENDPOINT_HTTPS = "https://localhost:8080/run";
}
