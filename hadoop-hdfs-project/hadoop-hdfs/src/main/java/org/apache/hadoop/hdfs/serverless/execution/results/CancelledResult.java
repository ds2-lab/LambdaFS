package org.apache.hadoop.hdfs.serverless.execution.results;

/**
 * Used to cancel results.
 */
public class CancelledResult extends NameNodeResult {
    private static final long serialVersionUID = 6000749351001594894L;

    public static final CancelledResult instance = new CancelledResult();

    private CancelledResult() { }
}
