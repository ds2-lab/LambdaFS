package io.hops.events;

/**
 * Allows the HopsEventManager to notify classes that events have been received.
 */
public interface HopsEventListener {
    /**
     * Called when an event is received by the HopsEventManager.
     *
     * @param eventOperation The EventOperation object, from which we can extract event data.
     * @param eventName The name of the event.
     */
    void eventReceived(HopsEventOperation eventOperation, String eventName);
}
