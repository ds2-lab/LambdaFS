package org.apache.hadoop.hdfs.serverless.metrics;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Locale;

// "Op Name", "Start Time", "End Time", "Duration (ms)", "Deployment", "HTTP", "TCP"
public class OperationPerformed implements Serializable, Comparable<OperationPerformed> {
    /**
     * Compare OperationPerformed instances by their start time.
     */
    public static Comparator<OperationPerformed> BY_START_TIME = new Comparator<OperationPerformed>() {
        @Override
        public int compare(OperationPerformed o1, OperationPerformed o2) {
            return (int)(o1.startTime - o2.startTime);
        }
    };

    /**
     * Compare OperationPerformed instances by their end time.
     */
    public static Comparator<OperationPerformed> BY_END_TIME = new Comparator<OperationPerformed>() {
        @Override
        public int compare(OperationPerformed o1, OperationPerformed o2) {
            return (int)(o1.endTime - o2.endTime);
        }
    };

    private static final long serialVersionUID = -3094538262184661023L;

    private final String operationName;

    private final String requestId;

    private final long startTime;

    private long endTime;

    private long duration;

    private final String deployment;

    private final boolean issuedViaTcp;

    private final boolean issuedViaHttp;

    public OperationPerformed(String operationName, String requestId, long startTime, long endTime,
                              String deployment, boolean issuedViaHttp, boolean issuedViaTcp) {
        this.operationName = operationName;
        this.requestId = requestId;
        this.startTime = startTime / 1000000;
        this.endTime = endTime / 1000000;
        this.duration = endTime - startTime;
        this.deployment = deployment;
        this.issuedViaHttp = issuedViaHttp;
        this.issuedViaTcp = issuedViaTcp;
    }

    /**
     * Modify the endTime of this OperationPerformed instance.
     * This also recomputes this instance's `duration` field.
     *
     * @param endTime The end time in nanoSeconds.
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime / 1000000;
        this.duration = this.endTime - startTime;
    }

    public Object[] getAsArray() {
        return new Object[] {
                this.operationName, this.startTime, this.endTime, this.duration, this.deployment,
                this.issuedViaHttp, this.issuedViaTcp
        };
    }

    @Override
    public String toString() {
        String format = "%-16s %-38s %-20s %-20s %-8s %-5s %-5s %-5s";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-dd hh:mm:ss:SSS")
                .withLocale( Locale.US )
                .withZone( ZoneId.of("UTC"));

        return String.format(format, operationName, requestId, formatter.format(Instant.ofEpochMilli(startTime)),
                formatter.format(Instant.ofEpochMilli(endTime)), duration, deployment,
                (issuedViaHttp ? "HTTP" : "-"), (issuedViaTcp ? "TCP" : "-"));

//            return operationName + " \t" + Instant.ofEpochMilli(timeIssued).toString() + " \t" +
//                    (issuedViaHttp ? "HTTP" : "-") + " \t" + (issuedViaTcp ? "TCP" : "-");
    }

    /**
     * Compare two instances of OperationPerformed.
     * The comparison is based exclusively on their timeIssued field.
     */
    @Override
    public int compareTo(OperationPerformed op) {
        return Long.compare(endTime, op.endTime);
    }

    public String getRequestId() {
        return requestId;
    }
}
