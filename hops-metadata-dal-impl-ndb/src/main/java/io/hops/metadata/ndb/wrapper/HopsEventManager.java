package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;
import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import io.hops.EventManager;
import io.hops.HopsEvent;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

/**
 * This class is responsible for listening to events from NDB and reacting to them appropriately.
 *
 * The events serve as cache invalidations for NameNodes. The NameNodes cache metadata locally in-memory. An Event
 * from NDB on the table for which the NameNode caches data serves to inform the NameNode that its cache is now
 * out-of-date.
 *
 * Currently implements the Singleton pattern, as there's no reason for a NameNode to have more than one
 * instance of this class at any given time.
 */
public class HopsEventManager implements EventManager {
    static final Log LOG = LogFactory.getLog(EventManager.class);

    private static HopsEventManager instance;

    /**
     * Name of the NDB table that contains the INodes.
     */
    private static final String INODES_TABLE_NAME = "hdfs_inodes";

    /**
     * The columns of the INode NDB table, in the order that they're defined in the schema.
     */
    private static final String[] INODE_TABLE_COLUMNS = new String[] {
            "partition_id", "parent_id", "name", "id", "user_id", "group_id", "modification_time",
            "access_time", "permission", "client_name", "client_machine", "client_node", "generation_stamp",
            "header", "symlink", "subtree_lock_owner", "size", "quota_enabled", "meta_enabled", "is_dir",
            "under_construction", "subtree_locked", "file_stored_in_db", "logical_time", "storage_policy",
            "children_num", "num_aces", "num_user_xattrs", "num_sys_xattrs"
    };

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
    private final HashMap<String, HopsEvent> eventMap;

    /**
     * All active EventOperation instances are contained in here.
     */
    private final HashMap<String, HopsEventOperation> eventOperationMap;

    /**
     * The active session with the database. Used to issue operations related to events,
     * and to receive events from the database.
     */
    private final HopsSession session;

    private HopsEventManager(HopsSession session) {
        this.session = session;

        this.eventMap = new HashMap<>();
        this.eventOperationMap = new HashMap<>();
    }

    private HopsEventManager() throws StorageException {
        this(ClusterjConnector.getInstance().obtainSession());
    }

    /**
     * Retrieve the singleton instance of HopsEventManager.
     *
     * @param session Active session with NDB.
     * @return singleton instance of HopsEventManager.
     */
    public HopsEventManager getInstance(HopsSession session) {
        if (instance == null)
            instance = new HopsEventManager(session);

        return instance;
    }

    /**
     * Retrieve the singleton instance of HopsEventManager.
     *
     * Creates the instance of it does not already exist.
     * @return singleton instance of HopsEventManager.
     */
    public HopsEventManager getInstance() throws StorageException {
        if (instance == null)
            instance = new HopsEventManager();

        return instance;
    }

    /**
     * Create and register an Event Operation for the specified event.
     *
     * @param eventName The name of the Event for which we're creating an EventOperation.
     */
    @Override
    public void createEventOperation(String eventName) throws StorageException {
        LOG.debug("Creating EventOperation for event " + eventName + " now...");

        EventOperation eventOperation;
        try {
            eventOperation = session.createEventOperation(eventName);
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        LOG.debug("Successfully created EventOperation for event " + eventName + ".");

        HopsEventOperation hopsEventOperation = new HopsEventOperation(eventOperation);
        eventOperationMap.put(eventName, hopsEventOperation);
    }

    /**
     * Unregister and drop the EventOperation associated with the given event from NDB.
     * @param eventName The unique identifier of the event whose EventOperation we wish to unregister.
     * @return True if an event operation was dropped, otherwise false.
     */
    @Override
    public boolean unregisterEventOperation(String eventName) throws StorageException {
        LOG.debug("Unregistering EventOperation for event " + eventName + " with NDB now...");

        // If we aren't tracking an event with the given name, then the argument is invalid.
        if (!eventOperationMap.containsKey(eventName))
            throw new IllegalArgumentException("There is no event operation associated with an event called "
                    + eventName + " currently being tracked by the EventManager.");

        HopsEventOperation hopsEventOperation = eventOperationMap.get(eventName);

        // Try to drop the event. If something goes wrong, we'll throw an exception.
        boolean dropped;
        try {
            dropped = session.dropEventOperation(hopsEventOperation.getClusterJEventOperation());
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        // If we failed to drop the event (probably because NDB doesn't think it exists), then the EventManager is
        // in an invalid state. That is, it was tracking some event called `eventName` that NDB does not know about.
        if (!dropped)
            throw new IllegalStateException("Failed to unregister EventOperation associated with event " + eventName +
                    " from NDB, despite the fact that the event operation exists within the EventManager.");

        // Make sure to remove the event from the eventMap now that it has been dropped by NDB.
        LOG.debug("Successfully unregistered EventOperation for event " + eventName + " with NDB!");
        eventOperationMap.remove(eventName);
        return true;
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
        LOG.debug("Successfully unregistered event " + eventName + " with NDB!");
        eventMap.remove(eventName);
        return true;
    }

    @Override
    public void run() {
        LOG.debug("The EventManager has started running.");

        // Loop forever, listening for events.
        while (true) {
            // As far as I can tell, this is NOT busy-waiting. This ultimately calls select(), or whatever
            // the equivalent is for the given operating system. And Linux, Windows, etc. suspend the
            // process if there are no file descriptors available, so this is not a busy wait.
            boolean events = session.pollForEvents(-1);

            if (!events) {
                LOG.debug("Received 0 events.");
                continue;
            }

            LOG.debug("Received at least one event!");
            int numEventsProcessed = processEvents();
            LOG.debug("Processed " + numEventsProcessed + " event(s).");
        }
    }

    /**
     * Called after `session.pollForEvents()` returns true.
     * @return the number of events that were processed.
     */
    @Override
    public synchronized int processEvents() {
        HopsEventOperation nextEventOp = session.nextEvent();
        int numEventsProcessed = 0;

        while (nextEventOp != null) {
            TableEvent eventType = nextEventOp.getEventType();
            LOG.debug("Event #" + numEventsProcessed + " of current batch has type " + eventType.name());

            nextEventOp = session.nextEvent();
            numEventsProcessed++;
        }

        return numEventsProcessed;
    }
}
