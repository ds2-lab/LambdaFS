package org.apache.hadoop.hdfs.serverless.execution.results;

/**
 * Used to cancel results.
 */
public class CancelledResult extends NameNodeResult {
    private static final long serialVersionUID = 6000749351001594894L;

    public static CancelledResult getInstance() {
        if (instance == null)
            instance = new CancelledResult();

        return instance;
    }

    private static CancelledResult instance;

    private CancelledResult() { }
}
