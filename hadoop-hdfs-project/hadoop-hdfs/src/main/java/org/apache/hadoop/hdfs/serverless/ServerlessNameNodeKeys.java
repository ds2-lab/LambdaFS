package org.apache.hadoop.hdfs.serverless;

/**
 * Global constants used as keys in HashMaps, JSON payloads, etc. by the ServerlessNameNode.
 */
public class ServerlessNameNodeKeys {
    public static final String DUPLICATE_REQUEST = "DUPLICATE-REQUEST";
    public static final String OPERATION = "op";
    public static final String CLIENT_NAME = "clientName";
    public static final String IS_CLIENT_INVOKER = "isClientInvoker";
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

    public static final String REQUEST_METHOD = "requestMethod";
    public static final String CANCELLED = "cancelled";
    public static final String REASON = "reason";
    public static final String SHOULD_RETRY = "shouldRetry";
    public static final String REASON_CONNECTION_LOST = "CONNECTION_LOST";
    public static final String OPENWHISK_ACTIVATION_ID = "OPENWHISK_ACTIVATION_ID";

    /**
     * Used to indicate to the NN that it should re-attempt this operation, even if
     * it was already completed, as the TCP connection was dropped before the result
     * could be delivered to the user.
     */
    public static final String FORCE_REDO = "forceRedo";

    public static final String COMMAND_LINE_ARGS = "command-line-arguments";
    public static final String DEBUG_NDB = "debugNdb";
    public static final String DEBUG_STRING_NDB = "debugStringNdb";
    public static final String CLIENT_INTERNAL_IP = "clientInternalIp";
    public static final String TCP_ENABLED = "tcpEnabled";

    public static final String FUNCTION_MAPPING = "FUNCTION_MAPPING";
    public static final String FILE_OR_DIR = "fileOrDirectory";
    public static final String PARENT_ID = "parentId";
    public static final String FUNCTION = "function";

}
