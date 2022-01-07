package io.hops.events;

import io.hops.exception.StorageException;

/**
 * Generic interface defining the API of the EventManager.
 *
 * This interface is specifically designed to be used with MySQL Cluster NDB, but theoretically
 * this interface could be implemented to work with Redis (for example).
 *
 * Concrete implementations of this class are expected to interface with some sort of caching mechanism. This
 * caching mechanism is responsible for managing the cache on the NameNode.
 *
 * The EventManager simply informs the cache that its data is out of date (i.e., it has been invalidated), and thus
 * it must be updated.
 *
 * The EventManager is expected to run as its own thread so that listening for events does not block other threads.
 * As such, it extends Runnable so that subclasses implement their own Run method.
 */
public interface EventManager extends Runnable {
    /**
     * Get the integer representation of the event types for ACK events.
     *
     * Basically, there are different kinds of events: UPDATE, INSERT, DELETE, etc.
     *
     * These have IDs associated with them. The IDs are hard-coded in the concrete implementation. So, we
     * provide this public API for accessing them, so clients who want to subscribe to events can specify
     * which types of events they're interested in.
     */
    public Integer[] getAckEventTypeIDs();

    /**
     * Get the event columns used in the ACK table event.
     *
     * TODO: This is not really generic...
     */
    public String[] getAckTableEventColumns();

    /**
     * Create and register an event with the given name.
     *
     * @param eventName Unique identifier of the event to be created.
     * @param recreateIfExists If true, delete and recreate the event if it already exists.
     * @param eventColumns The columns that are being monitored for the event.
     * @param tableName The name of the table on which the event is to be created.
     * @param alsoCreateSubscription If true, then we also create an event subscription after creating the event.
     * @param eventListener If non-null and 'alsoCreateSubscription' is true, then we'll add this as a listener
     *                      once the subscription is created.
     * @param tableEvents Event type IDs of the events we want to receive (e.g., INSERT, DELETE, UPDATE).
     *                    For now, these are hard-coded in the concrete implementation of this interface.
     *                    We reference the hard-coded values to specify them.
     *
     * @throws StorageException if something goes wrong when registering the event.
     * @return True if an event was created, otherwise false.
     */
    public EventRequestSignaler requestCreateEvent(String eventName, String tableName, String[] eventColumns,
                                        boolean recreateIfExists, boolean alsoCreateSubscription,
                                        HopsEventListener eventListener, Integer[] tableEvents)
            throws StorageException;

    /**
     * The calling thread waits on an internal semaphore until the Event Manager has finished its default setup.
     * This is used to ensure the calling thread does not add an event listener for events created during default
     * setup until the default setup has been completed.
     */
    public void waitUntilSetupDone() throws InterruptedException;

    /**
     * Listen for events.
     */
    @Override
    public void run();

    /**
     * Set the deployment number instance variable for this class.
     *
     * @param defaultEventName The name of the event to create/look for. Pass null to use the default. If null, will
     *      *                  default to {@link HopsEvent#INV_EVENT_NAME_BASE} + the local deployment number.
     * @param defaultDeleteIfExists Delete and recreate the event, if it already exists.
     * @param deploymentNumber The deployment number of the local serverless name node instance.
     * @param invalidationListener The event listener that will handle invalidations from intermediate storage.
     *                             Must not be null.
     */
    public void setConfigurationParameters(int deploymentNumber, String defaultEventName,
                                           boolean defaultDeleteIfExists, HopsEventListener invalidationListener);

    /**
     * Unregister and drop the EventOperation associated with the given event from NDB.
     *
     * The EventManager will immediately remove the listener object, but will retain the subscription for a short
     * period of time in case additional operations requiring the subscription are received.
     *
     * IMPORTANT: This should be called AFTER removing the event listener.
     *
     * The full order would be:
     *      createEventOperation()
     *      addListener()
     *      removeListener()
     *      unregisterEventOperation()
     *
     *       This also means that the `removeListener()` function should be called BEFORE calling the
     *       `unregisterEventOperation()` function!
     *
     * @param eventName The unique identifier of the event whose EventOperation we wish to unregister.
     * @param eventListener the event listener to be registered.
     */
    public void requestDropSubscription(String eventName, HopsEventListener eventListener) throws StorageException;
}
