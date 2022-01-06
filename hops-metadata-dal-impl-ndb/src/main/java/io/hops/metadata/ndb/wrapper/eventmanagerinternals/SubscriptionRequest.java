package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import io.hops.events.HopsEventListener;
import io.hops.metadata.ndb.wrapper.HopsEventManager;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates a request by one thread for the event manager thread to create or drop a
 * subscription for a particular event.
 *
 * Creating an EventOperation amounts to creating a subscription for a particular event. That subscription is tied
 * to the session that created it. This means that the {@link HopsEventManager} needs to create the subscriptions
 * itself using its own, private {@link HopsSession} instance.
 */
public final class SubscriptionRequest extends EventRequestBase {
    private static final long serialVersionUID = -6723933725262412978L;

    public enum SubscriptionOperation {
        /**
         * Indicates that we are to create a new event subscription.
         */
        CREATE_SUBSCRIPTION,

        /**
         * Indicates that we are to drop an existing event subscription.
         */
        DROP_SUBSCRIPTION
    }

    /**
     * The event listener associated with the event subscription that is being created/dropped.
     */
    private final HopsEventListener eventListener;

    /**
     * Indicates whether we are creating or dropping an event subscription.
     */
    private final SubscriptionOperation subscriptionOperation;

    /**
     * How many times we've tried and failed to create this event subscription.
     */
    private int failedAttempts;

    public SubscriptionRequest(String eventName, HopsEventListener eventListener, String[] eventColumns,
                               SubscriptionOperation subscriptionOperation) {
        super(eventName, eventColumns);

        this.eventListener = eventListener;
        this.subscriptionOperation = subscriptionOperation;
        this.failedAttempts = 0;
    }

    public int getNumFailedAttempts() {
        return failedAttempts;
    }

    /**
     * Set the internal flag to indicate that this request has been resubmitted.
     */
    public void incrementFailedAttempts() {
        this.failedAttempts++;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SubscriptionRequest))
            return false;

        SubscriptionRequest other = (SubscriptionRequest)o;

        return this.requestId.equals(other.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }

    public SubscriptionOperation getSubscriptionOperation() {
        return subscriptionOperation;
    }

    public HopsEventListener getEventListener() {
        return eventListener;
    }

    public String getEventName() {
        return eventName;
    }

    public String getRequestId() {
        return requestId;
    }
}
