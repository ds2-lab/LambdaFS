package io.hops.events;

/**
 * Exists to be used as a wrapper around ClusterJ's Event,
 * which itself is a wrapper around NDB's Event class.
 */
public class HopsEvent {
    /**
     * Event name that NameNodes use in order to watch for INode cache invalidations from NDB.
     */
    public static final String INODE_TABLE_EVENT_NAME = "namenode_cache_watch";

    /**
     * Event name for the invalidations table. Used for cache invalidations.
     */
    public static final String INV_TABLE_EVENT_NAME = "inv_table_watch";

    /**
     * Event name that NameNodes use during write operations/consistency protocol to ACK invalidations.
     */
    public static final String ACK_TABLE_EVENT_NAME = "ack_table_watch";

    private final String eventName;
    private final String tableName;
    private final int eventDurability;
    private final int eventReport;
    private final String[] eventColumns;

    public HopsEvent(String eventName, String tableName, int eventReport, int eventDurability, String[] eventColumns) {
        this.eventName = eventName;
        this.tableName = tableName;
        this.eventDurability = eventDurability;
        this.eventReport = eventReport;
        this.eventColumns = eventColumns;
    }

    public String getEventName() {
        return eventName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getEventDurability() {
        return eventDurability;
    }

    public int getEventReport() {
        return eventReport;
    }

    public String[] getEventColumns() {
        return eventColumns;
    }
}
