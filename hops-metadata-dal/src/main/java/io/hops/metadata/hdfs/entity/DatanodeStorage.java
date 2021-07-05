package io.hops.metadata.hdfs.entity;

/**
 * POJO encapsulating a DatanodeStorage. Used in conjunction with NDB.
 */
public class DatanodeStorage {

    /**
     * Used for printing a more readable value in the toString() function.
     */
    private static final String[] stateValues = {"NORMAL", "READ_ONLY_SHARED", "FAILED"};

    /**
     * Used for printing a more readable value in the toString() function.
     */
    private static final String[] storageTypeValues = {"DB", "SSD", "DISK", "RAID5", "ARCHIVE", "PROVIDED"};

    private final String storageId;

    private final int state;

    private final int storageType;

    public DatanodeStorage(String storageId, int state, int storageType) {
        this.storageId = storageId;
        this.state = state;
        this.storageType = storageType;
    }

    public String getStorageId() {
        return storageId;
    }

    public int getState() {
        return state;
    }

    public int getStorageType() {
        return storageType;
    }

    @Override
    public String toString() {
        return "DatanodeStorage < storageId = " + storageId + ", state = " + state + " (" + stateValues[state]
                + "), storageType = " + storageType + " (" + storageTypeValues[storageType] + ")>";
    }
}
