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

    public static final String COMMAND_LINE_ARGS = "command-line-arguments";
    public static final String DEBUG_NDB = "debugNdb";
    public static final String DEBUG_STRING_NDB = "debugStringNdb";
    public static final String CLIENT_INTERNAL_IP = "clientInternalIp";

    public static final String FUNCTION_MAPPING = "FUNCTION_MAPPING";
    public static final String FILE_OR_DIR = "fileOrDirectory";
    public static final String PARENT_ID = "parentId";
    public static final String FUNCTION = "function";

}
