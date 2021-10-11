package io.hops.metadata.hdfs.entity;

/**
 * POJO encapsulating a DataNode (specifically its metadata) as stored in the intermediate storage.
 */
public final class DataNodeMeta {

    /**
     * UUID identifying a given DataNode. For upgraded DataNodes, this is the
     * same as the StorageID that was previously used by the DataNode.
     * For newly formatted DataNodes, it is a UUID.
     */
    private final String datanodeUuid;

    /**
     * The hostname claimed by DataNode.
     */
    private final String hostname;

    /**
     * IP address of the DataNode.
     */
    private final String ipAddress;

    /**
     * Data streaming port.
     */
    private final int xferPort;

    /**
     * Info server port.
     */
    private final int infoPort;

    /**
     * IPC server port.
     */
    private final int ipcPort;

    private final int infoSecurePort;

    private final long creationTime;

    public DataNodeMeta(String datanodeUuid, String hostname, String ipAddress, int xferPort,
                        int infoPort, int infoSecurePort, int ipcPort, long creationTime) {
        this.datanodeUuid = datanodeUuid;
        this.hostname = hostname;
        this.ipAddress = ipAddress;
        this.xferPort = xferPort;
        this.infoPort = infoPort;
        this.ipcPort = ipcPort;
        this.infoSecurePort = infoSecurePort;
        this.creationTime = creationTime;
    }

    public String getDatanodeUuid() {
        return datanodeUuid;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getXferPort() {
        return xferPort;
    }

    public int getInfoPort() {
        return infoPort;
    }

    public int getIpcPort() {
        return ipcPort;
    }

    public int getInfoSecurePort() { return infoSecurePort; }

    @Override
    public String toString() {
        return "Datanode(UUID=" + datanodeUuid + ", hostname=" + hostname + ", ipAddress=" + ipAddress
                + ", xferPort=" + xferPort + ", infoPort=" + infoPort + ", infoSecurePort=" + infoSecurePort +
                ", ipcPort=" + ipcPort + ", creationTime=" + creationTime + ")";
    }

    public long getCreationTime() {
        return creationTime;
    }

    /**
     * This is implemented to mirror the DatanodeID class' `equals` method.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof DataNodeMeta))
            return false;

        DataNodeMeta other = (DataNodeMeta)obj;

        return this.ipAddress.equals(other.getIpAddress()) &&
                this.datanodeUuid.equals(other.datanodeUuid) &&
                this.xferPort == other.xferPort;
    }

    /**
     * Mimics the compareTo() function in some sense. If the creation times are the same, this returns 0.
     *
     * If the creation time of THIS instance is less, then this returns -1. If it is greater, then it returns 1.
     *
     * In that sense, the ordering imposed by this function is NOT consistent with equals().
     * @param other The other DN against which we are comparing.
     * @return 0 if both DNs were created at the same time, -1 if THIS instance was created earlier, 1 if THIS
     * instance was created later.
     */
    public int compareCreationTimes(DataNodeMeta other) {
        return Long.compare(this.creationTime, other.creationTime);
    }
}