package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.TableEvent;
import io.hops.events.EventManager;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Used just like {@link SubscriptionTask}. See that class for general explanation.
 */
public class EventTask {
    private final String requestId;
    private final String eventName;
    private final String tableName;
    private final String[] eventColumns;
    private final TableEvent[] tableEvents;
    private final boolean recreateIfExists;
    private final SubscriptionTask subscriptionTask;

    public EventTask(String eventName, String tableName, String[] eventColumns, boolean recreateIfExists,
                     SubscriptionTask subscriptionTask, Integer[] tableEvents) {
        this.requestId = UUID.randomUUID().toString();
        this.eventName = eventName;
        this.tableName = tableName;
        this.eventColumns = eventColumns;
        this.recreateIfExists = recreateIfExists;
        this.subscriptionTask = subscriptionTask;

        // Convert the integers to table events.
        if (tableEvents != null)
            this.tableEvents = Arrays.stream(tableEvents).map(TableEvent::convert).toArray(TableEvent[]::new);
        else
            this.tableEvents = null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EventTask))
            return false;

        EventTask other = (EventTask)o;

        return this.requestId.equals(other.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }

    public String getRequestId() {
        return requestId;
    }

    public String getEventName() {
        return eventName;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getEventColumns() {
        return eventColumns;
    }

    public boolean isRecreateIfExists() {
        return recreateIfExists;
    }

    public SubscriptionTask getSubscriptionTask() {
        return subscriptionTask;
    }

    public TableEvent[] getTableEvents() {
        return tableEvents;
    }
}
