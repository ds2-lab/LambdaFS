package io.hops.metadata.ndb;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;
import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.Event;
import io.hops.EventManager;
import io.hops.HopsEvent;
import io.hops.HopsEventOperation;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsExceptionHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

/**
 * This class is responsible for listening to events from NDB and reacting to them appropriately.
 *
 * The events serve as cache invalidations for NameNodes. The NameNodes cache metadata locally in-memory. An Event
 * from NDB on the table for which the NameNode caches data serves to inform the NameNode that its cache is now
 * out-of-date.
 */
public class EventManagerClusterJ implements EventManager {
    static final Log LOG = LogFactory.getLog(EventManager.class);

    /**
     * These are the events that all NameNodes subscribe to.
     */
    private static final TableEvent[] eventsToSubscribeTo = new TableEvent[] {
            TableEvent.INSERT,
            TableEvent.DELETE,
            TableEvent.UPDATE
    };

    /**
     * All registered events are contained in here.
     */
    private HashMap<String, HopsEvent> eventMap;

    /**
     * All active EventOperation instances are contained in here.
     */
    private HashMap<String, HopsEventOperation> eventOperationMap;

    /**
     * The active session with the database. Used to issue operations related to events,
     * and to receive events from the database.
     */
    private HopsSession session;

    public EventManagerClusterJ(HopsSession session) {
        this.session = session;
    }

    public EventManagerClusterJ() throws StorageException {
        this.session = DBSessionProvider.sessionFactory.getSession();
    }

    public void createEventOperation() {

    }

    /**
     * Create and register an event with the given name.
     * @param eventName Unique identifier of the event to be created.
     * @param recreateIfExisting If true, delete and recreate the event if it already exists.
     * @throws StorageException if something goes wrong when registering the event.
     * @return True if an event was created, otherwise false.
     */
    @Override
    public boolean registerEvent(String eventName, String tableName, boolean recreateIfExisting)
            throws StorageException {
        LOG.debug("Registering event " + eventName + " with NDB now...");

        // If we're already tracking this event and we aren't supposed to recreate it, then just return.
        if (eventMap.containsKey(eventName) && !recreateIfExisting) {
            LOG.debug("Event " + eventName + " is already being tracked by the EventManager.");
            return false;
        }

        // Try to create the event. If something goes wrong, we'll throw an exception.
        Event event;
        try {
            event = session.createAndRegisterEvent(eventName, tableName, eventsToSubscribeTo);
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        LOG.debug("Successfully registered event " + eventName + " with NDB!");

        // Create a HopsEvent object so the EventManager can keep track of this event.
        HopsEvent hopsEvent = new HopsEvent(
                event.getName(),
                event.getTableName(),
                EventReport.convert(event.getReport()),
                EventDurability.convert(event.getDurability()),
                event.getEventColumns());
        eventMap.put(eventName, hopsEvent);

        return true;
    }

    /**
     * Delete the event with the given name.
     * @param eventName Unique identifier of the event to be deleted.
     * @return True if an event with the given name was deleted, otherwise false.
     *
     * @throws StorageException if something goes wrong when unregistering the event.
     */
    @Override
    public boolean unregisterEvent(String eventName)
            throws StorageException, IllegalArgumentException, IllegalStateException {
        LOG.debug("Unregistering event " + eventName + " with NDB now...");

        // If we aren't tracking an event with the given name, then the argument is invalid.
        if (!eventMap.containsKey(eventName))
            throw new IllegalArgumentException("There is no event " + eventName +
                    " currently being tracked by the EventManager.");

        // Try to drop the event. If something goes wrong, we'll throw an exception.
        boolean dropped;
        try {
             dropped = session.dropEvent(eventName);
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        // If we failed to drop the event (probably because NDB doesn't think it exists), then the EventManager is
        // in an invalid state. That is, it was tracking some event called `eventName` that NDB does not know about.
        if (!dropped) {
            LOG.error("Failed to unregister event " + eventName +
                    " from NDB, despite the fact that the event exists within the EventManager...");

            throw new IllegalStateException("Failed to unregister event " + eventName +
                    " from NDB, despite the fact that the event exists within the EventManager.");
        }

        // Make sure to remove the event from the eventMap now that it has been dropped by NDB.
        LOG.debug("Successfully registered event " + eventName + " with NDB!");
        eventMap.remove(eventName);
        return true;
    }

    @Override
    public void run() {
        LOG.debug("The EventManager has started running.");

        // Loop forever, listening for events.
        while (true) {

        }
    }
}
