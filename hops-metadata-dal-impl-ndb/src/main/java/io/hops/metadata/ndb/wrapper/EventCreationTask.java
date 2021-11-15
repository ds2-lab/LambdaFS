package io.hops.metadata.ndb.wrapper;

import java.io.Serializable;

/**
 * Encapsulates a request by one thread for the event manager thread to create a
 * subscription for a particular event.
 *
 * Creating an EventOperation amounts to creating a subscription for a particular event. That subscription is tied
 * to the session that created it. This means that the {@link HopsEventManager} needs to create the subscriptions
 * itself using its own, private {@link HopsSession} instance.
 *
 * So when clients call {@link HopsEventManager#requestCreateEventSubscription(String)}, they're actually enqueuing an instance
 * of this class in an internal BlockingQueue of the Event Manager.
 */
public final class EventCreationTask implements Serializable {
    private static final long serialVersionUID = -6723933725262412978L;

    private String eventName;
}
