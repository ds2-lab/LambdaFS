package org.apache.hadoop.hdfs.serverless.zookeeper;

/**
 * Used by the ZooKeeper clients to invalidate cache when connection to the ensemble is lost.
 */
public interface Invalidatable {
    /**
     * Used to hook into whatever entity is invalidatable and invalidate their cache when connection to the ZK
     * ensemble is lost.
     */
    public void invalidateCache();
}
