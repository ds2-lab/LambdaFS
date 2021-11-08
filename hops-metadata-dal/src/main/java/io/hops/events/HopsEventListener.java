package io.hops.events;

/**
 * Allows the HopsEventManager to notify classes that events have been received.
 */
public interface HopsEventListener {
    /**
     * Called when an event is received by the HopsEventManager.
     *
     * @param eventData Data related to the event operation.
     * @param eventName The name of the event.
     */
    void eventReceived(Object eventData, String eventName);
}
