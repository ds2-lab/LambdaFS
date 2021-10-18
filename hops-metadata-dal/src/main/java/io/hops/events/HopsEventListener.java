package io.hops.events;

/**
 * Allows the HopsEventManager to notify classes that events have been received.
 */
public interface HopsEventListener {
    /**
     * Called when an event is received by the HopsEventManager.
     *
     * @param iNodeId The INode ID of the INode involved in the NDB operation that triggered the event.
     * @param shouldInvalidate If true, then this event should invalidate the associated metadata.
     */
    void eventReceived(long iNodeId, boolean shouldInvalidate);
}
