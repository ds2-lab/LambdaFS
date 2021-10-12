package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
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
 * TODO:
 *  For now, I am assuming that there is just ONE active event operation, and that event operation
 *  is using default column tables and record attributes, as defined below in the static variables.
 */
public class HopsEventManager implements EventManager {
    static final Log LOG = LogFactory.getLog(HopsEventManager.class);

    // private static HopsEventManager instance;

    /**
     * Default event name that NameNodes use in order to watch for cache invalidations from NDB.
     */
    private static final String DEFAULT_EVENT_NAME = "namenode_cache_watch";

    /**
     * Name of the NDB table that contains the INodes.
     */
    private static final String INODES_TABLE_NAME = "hdfs_inodes";

    /**
     * The columns of the INode NDB table, in the order that they're defined in the schema.
     *
     * TODO:
     *  We may want to add a column to keep track of who last modified the row so that NNs can determine
     *  whether or not they need to retrieve the data from NDB or not when receiving an event. That is, if NN 1
     *  receives an Event that the row was updated after they (NN1) performed the update, then obviously NN1 already
     *  has the updated metadata and shouldn't read anything from NDB. But if NN2 receives this event, then they would
     *  see that a different NN updated the row, and thus they'd retrieve the data from NDB.
     *
     * TODO:
     *  Also, only a few necessary columns should have their values read during Event reception. In addition to the
     *  column described above, NN's could also inspect the parent_id column to determine if the Event pertains to
     *  metadata that they are caching. If not, then the event can be safely ignored.
     */
    private static final String[] INODE_TABLE_COLUMNS = new String[] {
            "partition_id", "parent_id", "name", "id", "user_id", "group_id", "modification_time",
            "access_time", "permission", "client_name", "client_machine", "client_node", "generation_stamp",
            "header", "symlink", "subtree_lock_owner", "size", "quota_enabled", "meta_enabled", "is_dir",
            "under_construction", "subtree_locked", "file_stored_in_db", "logical_time", "storage_policy",
            "children_num", "num_aces", "num_user_xattrs", "num_sys_xattrs"
    };

    /**
     * The columns of the INode table for which we wish to see the pre-/post-values
     * when receiving an event on the INode table.
     *
     * These should just be numerical (32- or 64-bit integers, ideally).
     */
    private static final String[] INODE_TABLE_RECORD_ATTR_COLUMNS = new String[] {
            "partition_id", "parent_id", "id"
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
     * The active event operation. For now, we assume that there is just one active event
     * operation at any given time. In the future, we may expand this functionality and support
     * multiple concurrent event operations, assuming NDB supports this.
     */
    private HopsEventOperation activeEventOperation;

    /**
     * The active session with the database. Used to issue operations related to events,
     * and to receive events from the database.
     */
    private HopsSession session;

    /**
     * Used to track if default setup has been performed. In most cases, we'll want to just use the
     * default setup method, so we display a warning when that method has not been performed.
     */
    private boolean defaultSetupPerformed = false;

    public HopsEventManager() throws StorageException {
        this.session = ClusterjConnector.getInstance().obtainSession(true);
        this.eventMap = new HashMap<>();
        this.eventOperationMap = new HashMap<>();
    }

    /**
     * Retrieve the singleton instance of HopsEventManager.
     *
     * @param session Active session with NDB.
     * @return singleton instance of HopsEventManager.
     */
//    public HopsEventManager getInstance(HopsSession session) {
//        if (instance == null)
//            instance = new HopsEventManager(session);
//
//        return instance;
//    }

    /**
     * Retrieve the singleton instance of HopsEventManager.
     *
     * Creates the instance of it does not already exist.
     * @return singleton instance of HopsEventManager.
     */
//    public HopsEventManager getInstance() throws StorageException {
//        if (instance == null)
//            instance = new HopsEventManager();
//
//        return instance;
//    }

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

        HopsEventOperation hopsEventOperation = new HopsEventOperation(eventOperation, eventName);
        eventOperationMap.put(eventName, hopsEventOperation);
    }

    /**
     * Create an event operation for the event specified by the given event name and return it.
     *
     * This is for internal use only.
     * @param eventName The name of the event for which an event operation should be created.
     */
    private HopsEventOperation createAndReturnEventOperation(String eventName) throws StorageException {
        createEventOperation(eventName);

        return eventOperationMap.get(eventName);
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
     * @param recreateIfExists If true, delete and recreate the event if it already exists.
     * @throws StorageException if something goes wrong when registering the event.
     *
     * @return True if an event was created, otherwise false. Note that returning false does not
     * indicate that something definitely went wrong; rather, the event could just already exist.
     */
    @Override
    public boolean registerEvent(String eventName, String tableName, boolean recreateIfExists)
            throws StorageException {
        LOG.debug("Registering event " + eventName + " with NDB now...");

        // If we're already tracking this event, and we aren't supposed to recreate it, then just return.
        if (eventMap.containsKey(eventName) && !recreateIfExists) {
            LOG.debug("Event " + eventName + " is already being tracked by the EventManager.");
            return false;
        }

        // Try to create the event. If something goes wrong, we'll throw an exception.
        Event event;
        try {
            event = session.createAndRegisterEvent(eventName, tableName, eventsToSubscribeTo, recreateIfExists);
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

        if (!defaultSetupPerformed)
            LOG.warn("Default set-up has NOT been performed.");

        LOG.debug("Number of events created: " + eventMap.size());
        LOG.debug("Number of event operations created: " + eventOperationMap.size());

        // Loop forever, listening for events.
        while (true) {
            // As far as I can tell, this is NOT busy-waiting. This ultimately calls select(), or whatever
            // the equivalent is for the given operating system. And Linux, Windows, etc. suspend the
            // process if there are no file descriptors available, so this is not a busy wait.
            //
            // Also, I originally passed -1 here, but this did not appear to cause the event manager to block
            // for very long. Like less than a second, tens of milliseconds. So I just have it wait for a minute
            // before trying again.
            boolean events = session.pollForEvents(60000);

            // Just continue if we didn't find any events.
            if (!events)
                continue;

            LOG.debug("Received at least one event!");
            int numEventsProcessed = processEvents();
            LOG.debug("Processed " + numEventsProcessed + " event(s).");
        }
    }

    /**
     * Perform the default setup/initialization of the event and event operation.
     * @param eventName The name of the event to create/look for. Pass null to use the default.
     * @param deleteIfExists Delete and recreate the event, if it already exists.
     */
    @Override
    public void defaultSetup(String eventName, boolean deleteIfExists) throws StorageException {
        if (eventName == null)
            eventName = DEFAULT_EVENT_NAME;

        if (deleteIfExists)
            LOG.warn("Will delete and recreate event " + eventName + " if it already exists!");

        // This will add the event to the event map. We do NOT recreate the event if it already exists,
        // as the event could have been created by another NameNode. We would only want to recreate it
        // if we were changing something about the event's definition, and if all future NameNodes
        // expected this change, and if there were no other NameNodes currently using the event.
        boolean registeredSuccessfully = registerEvent(eventName, INODES_TABLE_NAME, false);

        if (!registeredSuccessfully) {
            LOG.error("Failed to successfully register default event " + eventName
                    + " on table " + INODES_TABLE_NAME);

            throw new StorageException("Failed to register event " + eventName + " on table " + INODES_TABLE_NAME);
        }

        LOG.debug("Creating event operation for event " + eventName + " now...");
        HopsEventOperation eventOperation = createAndReturnEventOperation(eventName);
        EventOperation clusterJEventOperation = eventOperation.getClusterJEventOperation();

        LOG.debug("Setting up record attributes for event " + eventName + " now...");
        for (String columnName : INODE_TABLE_RECORD_ATTR_COLUMNS) {
            boolean success = eventOperation.addRecordAttribute(columnName);

            if (!success)
                LOG.error("Failed to create record attribute(s) for column " + columnName + ".");
        }

        LOG.debug("Executing event operation for event " + eventName + " now...");
        clusterJEventOperation.execute();
        activeEventOperation = eventOperation;

        defaultSetupPerformed = true;
    }

    /**
     * Called after `session.pollForEvents()` returns true.
     * @return the number of events that were processed.
     */
    @Override
    public synchronized int processEvents() {
        HopsEventOperation nextEventOp = session.nextEvent("N/A");
        int numEventsProcessed = 0;

        while (nextEventOp != null) {
            TableEvent eventType = nextEventOp.getEventType();

            // TODO:
            //  Possibly check the pre/post values associated with the event to determine the necessity of retrieving
            //  full values from NDB. But in any case, receiving an Event means the cache needs to be updated.

            // TODO:
            //  For now, I am assuming that there is just ONE active event operation, and that event operation
            //  is using default column tables and record attributes, as defined above in the static variables.

            switch (eventType) {
                case INSERT:
                    LOG.debug("Received INSERT event from NDB.");
                    break;
                case DELETE:
                    LOG.debug("Received DELETE event from NDB.");
                    break;
                case UPDATE:
                    LOG.debug("Received UPDATE event from NDB.");
                    break;
                default:
                    LOG.debug("Received unexpected " + eventType.name() + " event from NDB.");
                    break;
            }

            // Print the pre- and post-values for the columns for which record attributes were created.
            for (String columName : INODE_TABLE_RECORD_ATTR_COLUMNS) {
                long preValue = activeEventOperation.getLongPreValue(columName);
                long postValue = activeEventOperation.getLongPostValue(columName);

                LOG.debug("Pre-value for column " + columName + ": " + preValue);
                LOG.debug("Post-value for column " + columName + ": " + postValue);
            }

            // TODO:
            //  Determine whether or not to handle the event based on whether the parent INode ID
            //  caches to this INode (i.e., depending on whether this NameNode caches the relevant metadata or not).

            nextEventOp = session.nextEvent("N/A");
            numEventsProcessed++;
        }

        LOG.debug("Finished processing batch of " + numEventsProcessed + (numEventsProcessed == 1 ?
                " event" : " events") + " from NDB.");

        return numEventsProcessed;
    }
}
