package io.hops.metrics;

public class PhaseMetric {
    private long invocationTime;
    private long preprocessingTime;
    private long waitingInQueueTime;
    private long executionTime;
    private long postprocessingTime;
    private long returnToUserTime;

    public PhaseMetric() {

    }

    public PhaseMetric(long invocationTime, long preprocessingTime, long waitingInQueueTime, long executionTime,
                       long postprocessingTime, long returnToUserTime) {
        this.invocationTime = invocationTime;
        this.preprocessingTime = preprocessingTime;
        this.waitingInQueueTime = waitingInQueueTime;
        this.postprocessingTime = postprocessingTime;
        this.executionTime = executionTime;
        this.postprocessingTime = postprocessingTime;
        this.returnToUserTime = returnToUserTime;
    }
}
