package org.apache.hadoop.hdfs.server.namenode.metrics;

public class GarbageCollectionInfo {
    private long numGCs;
    private long gcTimeMs;

    public GarbageCollectionInfo(long numGCs, long gcTimeMs) {
        this.numGCs = numGCs;
        this.gcTimeMs = gcTimeMs;
    }

    public long getGcTimeMs() {
        return gcTimeMs;
    }

    public void setGcTimeMs(long gcTimeMs) {
        this.gcTimeMs = gcTimeMs;
    }

    public long getNumGCs() {
        return numGCs;
    }

    public void setNumGCs(long numGCs) {
        this.numGCs = numGCs;
    }

    @Override
    public String toString() {
        return "GarbageCollectionInfo[numGCs=" + numGCs + ", gcTimeMs=" + gcTimeMs + "ms]";
    }
}
