package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public abstract class EventRequestBase implements Serializable {
    private static final long serialVersionUID = 8118885038767110187L;

    /**
     * The unique ID of this request.
     */
    protected final String requestId;

    /**
     * The name of the event associated with this request.
     *
     * For event-level requests, this is just the name of the event to create (or whatever).
     * For subscription requests, this is the name of the event for which we are creating/dropping a subscription.
     */
    protected final String eventName;

    /**
     * The names of the NDB table columns that are monitored by the event associated with this request.
     */
    protected final String[] eventColumns;

    protected EventRequestBase(String eventName, String[] eventColumns) {
        this.requestId = UUID.randomUUID().toString();
        this.eventName = eventName;
        this.eventColumns = eventColumns;
    }

    /**
     * Return the unique ID of this request.
     */
    public String getRequestId() { return this.requestId; }

    /**
     * Return the name of the event associated with this request.
     *
     * For event-level requests, this is just the name of the event to create (or whatever).
     * For subscription requests, this is the name of the event for which we are creating/dropping a subscription.
     */
    public String getEventName() { return this.eventName; }

    /**
     * Return a list of the names of the NDB table columns that are monitored by the event associated with this request.
     */
    public String[] getEventColumns() { return this.eventColumns; }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EventRequestBase))
            return false;

        EventRequestBase other = (EventRequestBase)o;

        return this.requestId.equals(other.requestId);
    }
}
