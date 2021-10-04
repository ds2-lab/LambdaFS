package io.hops;

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
     * Create and register an event with the given name.
     * @param eventName Unique identifier of the event to be created.
     * @param recreateIfExisting If true, delete and recreate the event if it already exists.
     * @return The newly-created Event if successful.
     *
     * @throws StorageException if something goes wrong when registering the event.
     */
    public HopsEvent registerEvent(String eventName, String tableName, boolean recreateIfExisting) throws StorageException;

    /**
     * Delete the event with the given name.
     * @param eventName Unique identifier of the event to be deleted.
     * @return True if an event with the given name was deleted, otherwise false.
     *
     * @throws StorageException if something goes wrong when unregistering the event.
     */
    public boolean unregisterEvent(String eventName) throws StorageException;

    /**
     * Listen for events.
     */
    @Override
    public void run();
}
