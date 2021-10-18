package io.hops.events;

/**
 * Exists to be used as a wrapper around ClusterJ's Event,
 * which itself is a wrapper around NDB's Event class.
 */
public class HopsEvent {
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
