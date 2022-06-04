package org.apache.hadoop.hdfs.serverless.execution.results;

import java.io.Serializable;

/**
 * Used to return 'null' for a FileSystemTask. If this is the result that is obtained, then
 * null is returned in place of an actual object (i.e., something that implements Serializable).
 *
 * The HopsFS client code knows to return null if it receives an instance of this class as a result
 * from a file system operation.
 */
public class NullResult implements Serializable {
    private static final long serialVersionUID = -1663521618378958991L;

    private static NullResult instance;

    public static NullResult getInstance() {
        if (instance == null)
            instance = new NullResult();

        return instance;
    }
}