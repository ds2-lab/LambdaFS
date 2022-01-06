package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import com.mysql.clusterj.TableEvent;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Used just like {@link SubscriptionRequest}. See that class for general explanation.
 */
public class EventRequest extends EventRequestBase{
    private static final long serialVersionUID = -3995932876781735599L;

    /**
     * The name of the table on which the event will be created.
     */
    private final String tableName;

    /**
     * The type of actions/events that will trigger this event.
     */
    private final TableEvent[] tableEvents;

    /**
     * If true, then this event will be recreated if it already exists.
     * If false, then we will abort the operation if we find an event by the same name already exists.
     */
    private final boolean recreateIfExists;

    /**
     * If this is non-null, then we're all supposed to create a subscription to the event after creating it.
     *
     * TODO: If the event already exists and recreateIfExists is false and this field is non-null,
     *       do we still create the subscription, or does that not happen? It needs to happen.
     */
    private final SubscriptionRequest subscriptionTask;

    public EventRequest(String eventName, String tableName, String[] eventColumns, boolean recreateIfExists,
                        SubscriptionRequest subscriptionTask, Integer[] tableEvents) {
        super(eventName, eventColumns);

        this.tableName = tableName;
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
        if (!(o instanceof EventRequest))
            return false;

        EventRequest other = (EventRequest)o;

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

    public SubscriptionRequest getSubscriptionTask() {
        return subscriptionTask;
    }

    public TableEvent[] getTableEvents() {
        return tableEvents;
    }
}
