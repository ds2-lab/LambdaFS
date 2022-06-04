package org.apache.hadoop.hdfs.serverless.execution.results;

import java.io.Serializable;

/**
 * Encapsulates the mapping of a particular file or directory to a particular serverless function.
 */
public class ServerlessFunctionMapping  implements Serializable {
    private static final long serialVersionUID = 7649887040567903783L;

    /**
     * The file or directory that we're mapping to a serverless function.
     */
    public String fileOrDirectory;

    /**
     * The ID of the file or directory's parent iNode.
     */
    public long parentId;

    /**
     * The number of the serverless function to which the file or directory was mapped.
     */
    public int mappedFunctionNumber;

    public ServerlessFunctionMapping(String fileOrDirectory, long parentId, int mappedFunctionNumber) {
        this.fileOrDirectory = fileOrDirectory;
        this.parentId = parentId;
        this.mappedFunctionNumber = mappedFunctionNumber;
    }

    @Override
    public String toString() {
        return "FunctionMapping: [src=" + fileOrDirectory + ", parentId=" + parentId
                + ", targetFunctionNumber=" + mappedFunctionNumber + "]";
    }
}