package io.hops.metadata.ndb.wrapper;

import io.hops.events.HopsEventListener;

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
 *
 * So when clients call {@link HopsEventManager#requestCreateSubscription(String)}, they're actually enqueuing an instance
 * of this class in an internal BlockingQueue of the Event Manager.
 */
public final class SubscriptionTask implements Serializable {
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
     * Used to uniquely identify this request in case we receive multiple requests for the same event.
     *
     * (They probably wouldn't have the same exact event listener, but if they had NO event listener, then there
     * would be no way to distinguish them without this field.)
     */
    private final String requestId;

    /**
     * The name of the event for which a subscription is being created/dropped.
     */
    private final String eventName;

    /**
     * The event listener associated with the event subscription that is being created/dropped.
     */
    private final HopsEventListener eventListener;

    /**
     * Indicates whether we are creating or dropping an event subscription.
     */
    private final SubscriptionOperation subscriptionOperation;

    public SubscriptionTask(String eventName, HopsEventListener eventListener,
                            SubscriptionOperation subscriptionOperation) {
        this.eventName = eventName;
        this.eventListener = eventListener;
        this.subscriptionOperation = subscriptionOperation;

        this.requestId = UUID.randomUUID().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SubscriptionTask))
            return false;

        SubscriptionTask other = (SubscriptionTask)o;

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
