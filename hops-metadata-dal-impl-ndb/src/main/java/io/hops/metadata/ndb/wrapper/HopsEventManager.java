package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import io.hops.events.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.ndb.ClusterjConnector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * This class is responsible for listening to events from NDB and reacting to them appropriately. This is how
 * the HopsFS codebase interfaces with events.
 *
 * The events serve as cache invalidations for NameNodes. The NameNodes cache metadata locally in-memory. An Event
 * from NDB on the table for which the NameNode caches data serves to inform the NameNode that its cache is now
 * out-of-date.
 *
 * Originally, we assumed that there would just be one active event operation at any given time. This assumption
 * no longer holds. All NameNodes subscribe to event notifications on the `hdfs_inodes` table, and during write
 * operations, the leader protocol subscribes to events on the `write_acknowledgements` table during the carrying
 * out of the consistency protocol. This required rewriting certain aspects of the class and also updating the
 * ClusterJ API to allow for equality checks between instances of NdbEventOperation objects.
 */
public class HopsEventManager implements EventManager {
    private static final Log LOG = LogFactory.getLog(HopsEventManager.class);

    /**
     * How long we poll for events before giving up and checking if we have any requests to create event subscriptions.
     */
    private static final int POLL_TIME_MILLISECONDS = 50;

    /**
     * The capacity of the BlockingQueues in which requests to create/drop event subscriptions are placed.
     */
    private static final int REQUEST_QUEUE_CAPACITIES = 25;

    /**
     * The maximum number of requests to create a new event subscription that we'll process before stopping to
     * listen for more events.
     */
    private static final int MAX_SUBSCRIPTION_REQUESTS = REQUEST_QUEUE_CAPACITIES;

    /**
     * Used to signal that we're done with our set-up and it is safe to add event listeners for the
     * default event operation.
     */
    private final Semaphore defaultSetupSemaphore = new Semaphore(-1);

    /**
     * This session is used exclusively by the thread running the HopsEventManager. If another
     * thread accesses this session, then we'll likely crash due to how the C++ NDB library works.
     */
    private HopsSession session;

    /**
     * Name of the NDB table that contains the INodes.
     */
    private static final String INODES_TABLE_NAME = "hdfs_inodes";

    /**
     * Base name (i.e., name without the integer at the end) for the invalidations tables.
     */
    private static final String INV_TABLE_NAME_BASE = "invalidations_deployment";

    /**
     * Name of table used during write operations/consistency protocol to ACK invalidations.
     */
    private static final String ACK_TABLE_NAME_BASE = "write_acknowledgements";

    /**
     * Internal queue to keep track of requests to create/register events.
     */
    private final BlockingQueue<EventTask> createEventQueue;

    /**
     * Internal queue to keep track of requests to drop/unregister events.
     */
    private final BlockingQueue<EventTask> dropEventQueue;

    private final ConcurrentHashMap<EventTask, Semaphore> eventCreationNotifiers;

    private final ConcurrentHashMap<EventTask, Semaphore> eventRemovalNotifiers;

//    /**
//     * Internal queue used to keep track of requests to create event subscriptions.
//     *
//     * Creating an EventOperation amounts to creating a subscription for a particular event. That subscription is tied
//     * to the session that created it. This means that the {@link HopsEventManager} needs to create the subscriptions
//     * itself using its own, private {@link HopsSession} instance.
//     */
//    private final BlockingQueue<SubscriptionTask> createSubscriptionRequestQueue;

//    /**
//     * When somebody issues us a request to create an event subscription, we return a semaphore that we'll
//     * decrement when we create the operation for them. So they can block to make sure the subscription gets created
//     * before continuing, if desired.
//     */
//    private final ConcurrentHashMap<SubscriptionTask, Semaphore> subscriptionCreationNotifiers;

    /**
     * Internal queue used to keep track of requests to drop event subscriptions.
     */
    private final BlockingQueue<SubscriptionTask> dropSubscriptionRequestQueue;

    /**
     * When somebody issues us a request to drop an event subscription, we return a semaphore that we'll
     * decrement when we drop the operation for them. So they can block to make sure the subscription gets dropped
     * before continuing, if desired.
     */
    private final ConcurrentHashMap<SubscriptionTask, Semaphore> subscriptionDeletionNotifiers;

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
    private static final String[] INODE_TABLE_EVENT_COLUMNS = new String[] {
            TablesDef.INodeTableDef.PARTITION_ID,     // int(11)
            TablesDef.INodeTableDef.PARENT_ID,        // int(11)
            TablesDef.INodeTableDef.ID                // int(11)
    };

    /**
     * Columns to use for the invalidation table event.
     */
    private static final String[] INV_TABLE_EVENT_COLUMNS = new String[] {
        TablesDef.InvalidationTablesDef.INODE_ID,        // bigint(20)
        TablesDef.InvalidationTablesDef.PARENT_ID,       // bigint(20)
        TablesDef.InvalidationTablesDef.LEADER_ID,       // bigint(20), so it's a long.
        TablesDef.InvalidationTablesDef.TX_START,        // bigint(20), so it's a long.
        TablesDef.InvalidationTablesDef.OPERATION_ID,    // bigint(20), so it's a long.
    };

    /**
     * Columns for which we want to see values for ACK events.
     */
    private static final String[] ACK_EVENT_COLUMNS = new String[] {
            TablesDef.WriteAcknowledgementsTableDef.NAME_NODE_ID,       // bigint(20)
            TablesDef.WriteAcknowledgementsTableDef.DEPLOYMENT_NUMBER,  // int(11)
            TablesDef.WriteAcknowledgementsTableDef.ACKNOWLEDGED,       // tinyint(4)
            TablesDef.WriteAcknowledgementsTableDef.OPERATION_ID        // bigint(20)
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
     * Classes who want to be notified that an event has occurred, so they can process the event.
     * This maps the name of the event to the event listeners for that event.
     */
    private final HashMap<String, List<HopsEventListener>> listenersMap = new HashMap<>();

    /**
     * Mapping from event operation to event columns, so we can print the columns when we receive an event.
     */
    private final HashMap<HopsEventOperation, String[]> eventColumnMap = new HashMap<>();

    /**
     * All registered events are contained in here.
     */
    private final HashMap<String, HopsEvent> eventMap;

    /**
     * All active EventOperation instances are contained in here.
     */
    private final HashMap<String, HopsEventOperation> eventOperationMap;

    /**
     * We also have a reverse mapping from the event operations to their names, as they do not store
     * their names in the object.
     */
    private final HashMap<HopsEventOperation, String> eventOpToNameMapping;

    /**
     * Name to use for the invalidation event when creating it/checking that it exists during defaultSetup().
     * This is set via the setConfigurationParameters() function.
     */
    private String defaultEventName;

    /**
     * If true, delete and recreate the invalidation event if it already exists during defaultSetup().
     * This is set via the setConfigurationParameters() function.
     */
    private boolean defaultDeleteIfExists;

    /**
     * Used during the default setup. This is the event listener responsible for handling cache invalidations.
     */
    private HopsEventListener invalidationListener;

    /**
     * Indicates that setConfiguration() has been called on this instance, which is a requirement.
     */
    private boolean configurationSet;

    /**
     * The deployment number of the local serverless name node instance. Set by calling defaultSetup().
     */
    private int deploymentNumber = -1;

    public HopsEventManager() {
        this.eventMap = new HashMap<>();
        this.eventOperationMap = new HashMap<>();
        this.eventOpToNameMapping = new HashMap<>();
//        this.createSubscriptionRequestQueue = new ArrayBlockingQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.dropSubscriptionRequestQueue = new ArrayBlockingQueue<>(REQUEST_QUEUE_CAPACITIES);
//        this.subscriptionCreationNotifiers = new ConcurrentHashMap<>();
        this.subscriptionDeletionNotifiers = new ConcurrentHashMap<>();
        this.createEventQueue = new ArrayBlockingQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.dropEventQueue = new ArrayBlockingQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.eventCreationNotifiers = new ConcurrentHashMap<>();
        this.eventRemovalNotifiers = new ConcurrentHashMap<>();
    }

    private HopsSession obtainSession() throws StorageException {
        return this.session;
    }

    /**
     * Register an event listener.
     *
     * @param listener the event listener to be registered.
     * @param eventName the name of the event for which we're registering an event listener.
     */
    private void addListener(String eventName, HopsEventListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("The event listener cannot be null.");

        LOG.debug("Adding Hops event listener for event " + eventName + ".");

        List<HopsEventListener> eventListeners;
        if (this.listenersMap.containsKey(eventName)) {
            eventListeners = this.listenersMap.get(eventName);
        } else {
            eventListeners = new ArrayList<HopsEventListener>();
            this.listenersMap.put(eventName, eventListeners);
        }

        eventListeners.add(listener);
        // updateCount(eventName, true);

        LOG.debug("Registered new event listener. Number of listeners: " + listenersMap.size() + ".");
    }

//    /**
//     * Update the count of listeners associated with a given event (really, event operation).
//     * @param eventName The name of the event.
//     * @param increment Whether we're incrementing or decrementing the count.
//     */
//    private synchronized void updateCount(String eventName, boolean increment) {
//        HopsEventOperation eventOperation = eventOperationMap.get(eventName);
//
//        if (eventOperation == null)
//            throw new IllegalStateException("Cannot update count for event " + eventName +
//                    ". Adding an even listener before registering the event operation is not permitted. Please " +
//                    "register the event operation first, then add the event listener afterwards.");
//
//        int currentCount = numListenersMapping.getOrDefault(eventOperation, 0);
//        int newCount;
//        if (increment)
//            newCount = currentCount + 1;
//        else
//            newCount = currentCount - 1;
//
//        assert(newCount >= 0);
//
//        if (currentCount == 1)
//            LOG.debug("There was 1 existing event listener for event operation " + eventName + ". Now there are " +
//                    newCount + ".");
//        else
//            LOG.debug("There were " + currentCount + " listeners for event op " + eventName + ". Now there are " +
//                    newCount + ".");
//
//        numListenersMapping.put(eventOperation, newCount);
//    }

    /**
     * Unregister an event listener.
     *
     * @param listener the event listener to be unregistered.
     * @param eventName the name of the event for which we're unregistering an event listener.
     *
     * @throws IllegalArgumentException If we do not have the provided listener registered with the specified event.
     */
    private void removeListener(String eventName, HopsEventListener listener) {
        List<HopsEventListener> eventListeners;
        if (this.listenersMap.containsKey(eventName)) {
            eventListeners = this.listenersMap.get(eventName);

            if (!eventListeners.contains(listener))
                throw new IllegalArgumentException("The provided event listener is not registered with " +
                        eventName + "!");

            eventListeners.remove(listener);
            // updateCount(eventName, false);
        } else {
            throw new IllegalArgumentException("We have no event listeners registered for event " + eventName + "!");
        }
    }

    private static final List<String> allHashCodeHexStringsEverCreated = new ArrayList<String>();
    private static final Map<String, String> hashCodeHexStringToEventName = new HashMap<>();
    // private static Lock _mutex = new ReentrantLock();


//    @Override
//    public Semaphore requestCreateSubscriptionWithListener(String eventName, HopsEventListener listener)
//            throws StorageException {
//        LOG.debug("Issuing request to create subscription for event " + eventName + " now.");
//
//        SubscriptionTask createSubscriptionRequest = new SubscriptionTask(eventName, listener,
//                SubscriptionTask.SubscriptionOperation.CREATE_SUBSCRIPTION);
//
//        try {
//            createSubscriptionRequestQueue.put(createSubscriptionRequest);
//        } catch (InterruptedException e) {
//            throw new StorageException("Failed to enqueue request to create subscription for event " +
//                    eventName + " due to interrupted exception:", e);
//        }
//
//        Semaphore creationNotifier = new Semaphore(0);
//        subscriptionCreationNotifiers.put(createSubscriptionRequest, creationNotifier);
//
//        return creationNotifier;
//    }

    // See the inherited JavaDoc for important information about why this function is designed the way it is.
//    @Override
//    public Semaphore requestCreateSubscription(String eventName) throws StorageException {
//
//        LOG.debug("Issuing request to create subscription for event " + eventName + " now.");
//
//        SubscriptionTask createSubscriptionRequest = new SubscriptionTask(eventName, null,
//                SubscriptionTask.SubscriptionOperation.CREATE_SUBSCRIPTION);
//
//        try {
//            createSubscriptionRequestQueue.put(createSubscriptionRequest);
//        } catch (InterruptedException e) {
//            throw new StorageException("Failed to enqueue request to create subscription for event " +
//                    eventName + " due to interrupted exception:", e);
//        }
//
//        Semaphore creationNotifier = new Semaphore(0);
//        subscriptionCreationNotifiers.put(createSubscriptionRequest, creationNotifier);
//
//        return creationNotifier;
//    }

    /**
     * This should only be called internally by whatever thread is running this HopsEventManager instance.
     * @param eventName The name of the event for which we are creating a subscription.
     */
    private void createEventOperation(String eventName) throws StorageException {
        HopsEventOperation hopsEventOperation;
        try {
            hopsEventOperation = obtainSession().createEventOperation(eventName);
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        LOG.debug("Successfully created EventOperation for event " + eventName + ": " +
                Integer.toHexString(hopsEventOperation.hashCode()));

        eventOperationMap.put(eventName, hopsEventOperation);
        eventOpToNameMapping.put(hopsEventOperation, eventName);

        if (eventName.equals(HopsEvent.ACK_EVENT_NAME_BASE))
            eventColumnMap.put(hopsEventOperation, ACK_EVENT_COLUMNS);
        else if (eventName.equals(HopsEvent.INV_EVENT_NAME_BASE))
            eventColumnMap.put(hopsEventOperation, INV_TABLE_EVENT_COLUMNS);

        allHashCodeHexStringsEverCreated.add(Integer.toHexString(hopsEventOperation.hashCode()));
        hashCodeHexStringToEventName.put(Integer.toHexString(hopsEventOperation.hashCode()), eventName);
    }

    /**
     * Create an event operation for the event specified by the given event name and return it.
     *
     * This function is for internal use only. It directly calls the `createEventOperation()` function, which
     * actually creates an EventOperaiton using the HopsEventMangaer's private, internal HopsSession instance. As
     * a result, if this function is somehow accessed by another thread, the subscription created will never
     * actually be used (since it will be created using another thread's HopsSession, which presumably won't
     * ever be checked for events).
     *
     * This is for internal use only.
     * @param eventName The name of the event for which an event operation should be created.
     */
    private HopsEventOperation createAndReturnEventOperation(String eventName) throws StorageException {
        createEventOperation(eventName);

        return eventOperationMap.get(eventName);
    }

    /**
     * Actually unregister an event operation. This should only be used internally, for the same reasons that the
     * createEventOperation function is only used internally.
     * @param eventName Name associated with the event operation that is being unregistered.
     * @param eventListener EventListener associated with the event we're removing.
     */
    private void unregisterEventOperation(String eventName, HopsEventListener eventListener) throws StorageException {
        LOG.debug("Unregistering EventOperation for event " + eventName + " with NDB now...");

        // If we aren't tracking an event with the given name, then the argument is invalid.
        if (!eventOperationMap.containsKey(eventName))
            throw new IllegalArgumentException("There is no event operation associated with an event called "
                    + eventName + " currently being tracked by the EventManager.");

        // First, unregister the event listener, as we do that no matter what.
        if (eventListener != null)
            removeListener(eventName, eventListener);

        HopsEventOperation hopsEventOperation = eventOperationMap.get(eventName);

        List<HopsEventListener> listeners = listenersMap.get(eventName);
        int currentNumberOfListeners = (listeners == null ? 0 : listeners.size());
        if (currentNumberOfListeners > 0) {
            LOG.debug("There are still " + currentNumberOfListeners + " event listener(s) for event " +
                    eventName + ". Will not drop the subscription. (Only removed the event listener.)");
            return;
        } else {
            LOG.debug("There are no more event listeners associated with the event. Dropping subscription.");
        }

        // Make sure to remove the event from the eventMap.
        eventOperationMap.remove(eventName);
        eventOpToNameMapping.remove(hopsEventOperation);

        // Try to drop the event. If something goes wrong, we'll throw an exception.
        boolean dropped;
        try {
            dropped = obtainSession().dropEventOperation(hopsEventOperation);
        } catch (ClusterJException e) {
            throw HopsExceptionHelper.wrap(e);
        }

        // If we failed to drop the event (probably because NDB doesn't think it exists), then the EventManager is
        // in an invalid state. That is, it was tracking some event called `eventName` that NDB does not know about.
        if (!dropped)
            throw new IllegalStateException("Failed to unregister EventOperation associated with event " + eventName +
                    " from NDB, despite the fact that the event operation exists within the EventManager.");

        LOG.debug("Successfully unregistered EventOperation for event " + eventName + " with NDB!");
    }

    @Override
    public Semaphore requestDropSubscription(String eventName, HopsEventListener eventListener) throws StorageException {
        LOG.debug("Issuing request to drop subscription for event " + eventName + " now...");

        SubscriptionTask dropRequest = new SubscriptionTask(eventName, eventListener,
                SubscriptionTask.SubscriptionOperation.DROP_SUBSCRIPTION);

        try {
            dropSubscriptionRequestQueue.put(dropRequest);
        } catch (InterruptedException e) {
            throw new StorageException("Failed to enqueue request to drop subscription for event " +
                    eventName + " due to interrupted exception:", e);
        }

        Semaphore dropNotifier = new Semaphore(0);
        subscriptionDeletionNotifiers.put(dropRequest, dropNotifier);

        return dropNotifier;
    }

    @Override
    public String[] getInvTableEventColumns() {
        return INV_TABLE_EVENT_COLUMNS;
    }

    @Override
    public String[] getAckTableEventColumns() {
        return ACK_EVENT_COLUMNS;
    }

    /**
     * Register an event with intermediate storage.
     * @param eventName The name of the event.
     * @param tableName The table on which the event will be registered.
     * @param eventColumns The columns of the table that will be involved with the event.
     * @param recreateIfExists If true, recreate the event if it already exists.
     */
    private void registerEvent(String eventName, String tableName, String[] eventColumns,
                               boolean recreateIfExists) throws StorageException {
        LOG.debug("Registering event " + eventName + " on table " + tableName + " with NDB now...");

        // If we're already tracking this event, and we aren't supposed to recreate it, then just return.
        if (eventMap.containsKey(eventName) && !recreateIfExists)
            LOG.debug("Event " + eventName + " is already being tracked by the EventManager.");

        // Try to create the event. If something goes wrong, we'll throw an exception.
        Event event;
        try {
            event = obtainSession().createAndRegisterEvent(eventName, tableName, eventColumns,
                    eventsToSubscribeTo, recreateIfExists);
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
    }

    /**
     * Register the event encapsulated by the given {@link EventTask} instance.
     * @param eventTask Represents the event to be created.
     */
    private void registerEvent(EventTask eventTask) throws StorageException {
        String eventName = eventTask.getEventName();
        String tableName = eventTask.getTableName();
        String[] eventColumns =  eventTask.getEventColumns();
        boolean recreateIfExists = eventTask.isRecreateIfExists();

        registerEvent(eventName, tableName, eventColumns, recreateIfExists);
    }

    /**
     * Unregister the event encapsulated by the given {@link EventTask} instance.
     * @param eventTask Represents the event to be unregistered.
     */
    private void unregisterEvent(EventTask eventTask) throws StorageException {
        String eventName = eventTask.getEventName();

        LOG.debug("Unregistering event " + eventName + " with NDB now...");

        // If we aren't tracking an event with the given name, then the argument is invalid.
        if (!eventMap.containsKey(eventName))
            throw new IllegalArgumentException("There is no event " + eventName +
                    " currently being tracked by the EventManager.");

        // Try to drop the event. If something goes wrong, we'll throw an exception.
        boolean dropped;
        try {
            dropped = obtainSession().dropEvent(eventName);
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
    }

    @Override
    public Semaphore requestRegisterEvent(String eventName, String tableName, String[] eventColumns,
                                          boolean recreateIfExists, boolean alsoCreateSubscription,
                                          HopsEventListener eventListener) throws StorageException {
        if (!alsoCreateSubscription && eventListener != null) {
            LOG.error("Indicated that no subscription should be created for event " + eventName +
                    ", but also provided an event listener...");
            throw new IllegalStateException("Event Listener should be null when 'alsoCreateSubscription' is false.");
        }

        // Start at 0. This way, creating the event will add a permit. If 'alsoCreateSubscription' is true,
        // then we decrement this by 1 so that the Semaphore will block until both the event is created and the
        // subscription is created. If eventListener is non-null, then we decrement it again so it blocks until
        // the listener is added too.
        int startingPermits = 0;

        SubscriptionTask subscriptionTask = null;
        if (alsoCreateSubscription) {
            startingPermits--; // Block until subscription is created.
            subscriptionTask = new SubscriptionTask(eventName, eventListener,
                    SubscriptionTask.SubscriptionOperation.CREATE_SUBSCRIPTION);

            if (eventListener != null)
                startingPermits--; // Block until listener is added too.
        }

        // The 'subscriptionTask' parameter will be null if `alsoCreateSubscription` is false.
        EventTask eventRegistrationTask =
                new EventTask(eventName, tableName, eventColumns, recreateIfExists, subscriptionTask);

        try {
            createEventQueue.put(eventRegistrationTask);
        } catch (InterruptedException e) {
            throw new StorageException("Failed to enqueue request to drop subscription for event " +
                    eventName + " due to interrupted exception:", e);
        }

        Semaphore eventRegisteredNotifier = new Semaphore(startingPermits);
        eventCreationNotifiers.put(eventRegistrationTask, eventRegisteredNotifier);

        return eventRegisteredNotifier;
    }

    /**
     * Delete the event with the given name.
     * @param eventName Unique identifier of the event to be deleted.
     * @return True if an event with the given name was deleted, otherwise false.
     *
     * @throws StorageException if something goes wrong when unregistering the event.
     */
    @Override
    public Semaphore requestUnregisterEvent(String eventName)
            throws StorageException, IllegalArgumentException, IllegalStateException {
        EventTask unregisterEventTask = new EventTask(eventName, null, null,
                false, null);

        try {
            dropEventQueue.put(unregisterEventTask);
        } catch (InterruptedException e) {
            throw new StorageException("Failed to enqueue request to drop subscription for event " +
                    eventName + " due to interrupted exception:", e);
        }

        Semaphore eventDroppedNotifier = new Semaphore(0);
        eventRemovalNotifiers.put(unregisterEventTask, eventDroppedNotifier);

        return eventDroppedNotifier;
    }

    @Override
    public void waitUntilSetupDone() throws InterruptedException {
        defaultSetupSemaphore.acquire();
    }

    /**
     * Process any requests we've received to register events with NDB. Note that these requests may contain
     * subscription-creation requests, which will also be processed by this function.
     */
    private void processEventRegistrationRequests() {
        int requestsProcessed = 0;
        while (createEventQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            EventTask eventRegistrationTask = createEventQueue.poll();
            LOG.debug("Processing event creation request for event " + eventRegistrationTask.getEventName());

            try {
                registerEvent(eventRegistrationTask);
            } catch (StorageException e) {
                LOG.error("Failed to register event " + eventRegistrationTask.getEventName() + ": ", e);
            }

            requestsProcessed++;

            Semaphore eventRegisteredNotifier = eventCreationNotifiers.get(eventRegistrationTask);

            if (eventRegisteredNotifier == null)
                throw new IllegalStateException("We do not have an subscription creation notifier for event " +
                        eventRegistrationTask.getEventName() + "!");

            // If the EventTask has an included SubscriptionTask, then the Semaphore would've been initialized
            // with -1 permits, so the other thread would still be blocked if it called .acquire() on the Semaphore.
            eventRegisteredNotifier.release(1);

            SubscriptionTask subscriptionTask = eventRegistrationTask.getSubscriptionTask();
            if (subscriptionTask != null)
                processCreateSubscriptionTask(subscriptionTask, eventRegisteredNotifier);
        }
    }

    /**
     * Process any requests we've received to unregister events with NDB.
     */
    private void processEventUnregistrationRequests() {
        int requestsProcessed = 0;
        while (dropEventQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            EventTask eventDropTask = dropEventQueue.poll();

            try {
                unregisterEvent(eventDropTask);
            } catch (StorageException e) {
                LOG.error("Failed to unregister event " + eventDropTask.getEventName() + ": ", e);
            }

            Semaphore eventDroppedNotifier = eventRemovalNotifiers.get(eventDropTask);

            if (eventDroppedNotifier == null)
                throw new IllegalStateException("We do not have an subscription creation notifier for event " +
                        eventDropTask.getEventName() + "!");

            eventDroppedNotifier.release(1);

            requestsProcessed++;
        }
    }

    /**
     * Process any requests we've received to create event subscriptions.
     */
//    private void processCreateSubscriptionRequests() {
//        int requestsProcessed = 0;
//        while (createSubscriptionRequestQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
//            SubscriptionTask createSubscriptionRequest = createSubscriptionRequestQueue.poll();
//            processCreateSubscriptionTask(createSubscriptionRequest);
//            requestsProcessed++;
//        }
//    }

    /**
     * Process a SubscriptionTask to create a subscription for an existing event.
     *
     * IMPORTANT: This calls release(1) on the Semaphore associated with the SubscriptionTask instance.
     *
     * @param createSubscriptionRequest The task that we're processing.
     * @param notifier The Semaphore for this subscription. It's the same Semaphore used for the event itself.
     */
    private void processCreateSubscriptionTask(SubscriptionTask createSubscriptionRequest, Semaphore notifier) {
        String eventName = createSubscriptionRequest.getEventName();
        LOG.debug("Processing subscription creation request for event " + eventName);
        HopsEventListener eventListener = createSubscriptionRequest.getEventListener();

        assert(createSubscriptionRequest.getSubscriptionOperation() ==
                SubscriptionTask.SubscriptionOperation.CREATE_SUBSCRIPTION);

        // If we get null, then just exit.
        if (eventName == null)
            return;

        HopsEventOperation eventOperation = null;
        try {
            eventOperation = createAndReturnEventOperation(eventName);
        } catch (StorageException e) {
            LOG.error("Encountered exception while trying to create event subscription for event " +
                    eventName + ":", e);
        }

        if (notifier == null)
            throw new IllegalStateException("We do not have an subscription creation notifier for event " +
                    eventName + "!");

        notifier.release(1);

        if (eventOperation != null) {
            eventOperation.execute();

            // If there's an event listener, we'll add it. Then release the semaphore again.
            if (eventListener != null) {
                addListener(eventName, eventListener);
                notifier.release(1);
            }
        }
    }

    /**
     * Process any requests we've received to drop event subscriptions.
     */
    public void processDropSubscriptionRequests() {
        int requestsProcessed = 0;
        while (dropSubscriptionRequestQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            SubscriptionTask dropSubscriptionRequest = dropSubscriptionRequestQueue.poll();
            String eventName = dropSubscriptionRequest.getEventName();
            HopsEventListener eventListener = dropSubscriptionRequest.getEventListener();

            assert(dropSubscriptionRequest.getSubscriptionOperation() ==
                    SubscriptionTask.SubscriptionOperation.DROP_SUBSCRIPTION);

            // If we get null, then just exit.
            if (eventName == null)
                break;

            try {
                unregisterEventOperation(eventName, eventListener);
            } catch (StorageException e) {
                LOG.error("Encountered exception while trying to create event subscription for event " +
                        eventName + ":", e);
            }

            requestsProcessed++;

            Semaphore droppedNotifier = subscriptionDeletionNotifiers.get(dropSubscriptionRequest);

            if (droppedNotifier == null)
                throw new IllegalStateException("We do not have an subscription dropped notifier for event " +
                        eventName + "!");

            droppedNotifier.release(1);
        }
    }

    @Override
    public void run() {
        LOG.debug("The EventManager has started running.");

        // We initialize the session instance variable within the run() method so that we obtain the session
        // in the thread that the Event Manager runs in.
        try {
            this.session = ClusterjConnector.getInstance().obtainSession();
        } catch (StorageException e) {
            LOG.error("Failed to obtain session for Event Manager:", e);
        }

        try {
            defaultSetup();
        } catch (StorageException e) {
            throw new IllegalStateException("Failed to initialize the HopsEventManager.");
        }

        LOG.debug("Number of events created: " + eventMap.size());
        LOG.debug("Number of event operations created: " + eventOperationMap.size());

        HopsSession session = null;
        try {
            session = obtainSession();
        } catch (StorageException ex) {
            LOG.error("Failed to acquire a HopsSession instance:", ex);
            throw new IllegalArgumentException("Failed to obtain a HopsSession instance; cannot poll for events.");
        }

        // Loop forever, listening for events.
        while (true) {
            // As far as I can tell, this is NOT busy-waiting. This ultimately calls select(), or whatever
            // the equivalent is for the given operating system. And Linux, Windows, etc. suspend the
            // process if there are no file descriptors available, so this is not a busy wait.
            //
            // We check for a short amount of time. If no events are found, then we check and see if we have any
            // requests to create new event subscriptions. If we do, then we'll handle those for a bit before
            // checking for more events.
            boolean events = session.pollForEvents(POLL_TIME_MILLISECONDS);

            // Just continue if we didn't find any events.
            if (events) {
                LOG.debug("Received at least one event!");
                int numEventsProcessed = processEvents(session);
                LOG.debug("Processed " + numEventsProcessed + " event(s).");
            }

            processEventRegistrationRequests();     // Process requests to create/register new events.
            processEventUnregistrationRequests();   // Process requests to drop/unregister events.

            //processCreateSubscriptionRequests();    // Process requests to create subscriptions.
            processDropSubscriptionRequests();      // Process requests to drop subscriptions.
        }
    }

    @Override
    public void setConfigurationParameters(int deploymentNumber, String defaultEventName,
                                           boolean defaultDeleteIfExists, HopsEventListener invalidationListener) {
        if (deploymentNumber < 0)
            throw new IllegalArgumentException("Deployment number must be greater than zero.");

        if (defaultEventName == null)
            defaultEventName = HopsEvent.INV_EVENT_NAME_BASE + deploymentNumber;

        if (invalidationListener == null)
            throw new IllegalArgumentException("The invalidation event listener must be non-null.");

        this.deploymentNumber = deploymentNumber;
        this.defaultEventName = defaultEventName;
        this.defaultDeleteIfExists = defaultDeleteIfExists;
        this.invalidationListener = invalidationListener;

        this.configurationSet = true;
    }

    /**
     * Perform the default setup/initialization of the `hdfs_inodes` table event and event operation.
     */
    private void defaultSetup() throws StorageException {
        if (!configurationSet)
            throw new StorageException("Must call setConfiguration() on the event manager before it begins running!");

        if (defaultEventName == null)
            defaultEventName = HopsEvent.INV_EVENT_NAME_BASE + deploymentNumber;

        if (defaultDeleteIfExists)
            LOG.warn("Will delete and recreate event " + defaultEventName + " if it already exists!");

        String tableName = INV_TABLE_NAME_BASE + deploymentNumber;

        // This will add the event to the event map. We do NOT recreate the event if it already exists,
        // as the event could have been created by another NameNode. We would only want to recreate it
        // if we were changing something about the event's definition, and if all future NameNodes
        // expected this change, and if there were no other NameNodes currently using the event.
        registerEvent(defaultEventName, tableName, INV_TABLE_EVENT_COLUMNS, defaultDeleteIfExists);

//        if (!registeredSuccessfully) {
//            LOG.error("Failed to successfully register default event " + defaultEventName
//                    + " on table " + tableName);
//
//            throw new StorageException("Failed to register event " + defaultEventName + " on table " + tableName);
//        }

        LOG.debug("Creating event operation for event " + defaultEventName + " now...");
        HopsEventOperation eventOperation = createAndReturnEventOperation(defaultEventName);

        LOG.debug("Setting up record attributes for event " + defaultEventName + " now...");
        for (String columnName : INV_TABLE_EVENT_COLUMNS) {
            boolean success = eventOperation.addRecordAttribute(columnName);

            if (!success)
                LOG.error("Failed to create record attribute(s) for column " + columnName + ".");
        }

        addListener(defaultEventName, invalidationListener);

        LOG.debug("Executing event operation for event " + defaultEventName + " now...");
        eventOperation.execute();

        // Signal that we're done with this. Just add a ton of permits.
        defaultSetupSemaphore.release(Integer.MAX_VALUE - 10);
    }

    /**
     * Called after `session.pollForEvents()` returns true.
     * @return the number of events that were processed.
     */
    private synchronized int processEvents(HopsSession session) {
        HopsEventOperation nextEventOp = session.nextEvent(null);
        int numEventsProcessed = 0;

        while (nextEventOp != null) {
            String eventType = nextEventOp.getEventType();

            LOG.debug("Received " + eventType + " event from NDB: " +
                    Integer.toHexString(nextEventOp.hashCode()) + ".");

            // Print the columns if we can determine what their names are from our mapping.
            if (eventColumnMap.containsKey(nextEventOp)) {
                String[] eventColumnNames = eventColumnMap.get(nextEventOp);

                // Print the pre- and post-values for the columns for which record attributes were created.
                for (String columnName : eventColumnNames) {
                    long preValue = nextEventOp.getLongPreValue(columnName);
                    long postValue = nextEventOp.getLongPostValue(columnName);

                    LOG.debug("Pre-value for column " + columnName + ": " + preValue);
                    LOG.debug("Post-value for column " + columnName + ": " + postValue);
                }
            }

            switch (eventType) {
                case HopsEventType.DELETE:    // Do nothing for now.
                    break;
                case HopsEventType.UPDATE:    // Fall through.
                case HopsEventType.INSERT:
                    notifyEventListeners(nextEventOp);
                    break;
                default:
                    LOG.debug("Received unexpected " + eventType + " event from NDB.");
                    break;
            }

            // TODO: Should we call .setCanCallNextEvent(false) on the event we just processed to mark it as such?
            nextEventOp = session.nextEvent(null);
            numEventsProcessed++;
        }

        LOG.debug("Finished processing batch of " + numEventsProcessed + (numEventsProcessed == 1 ?
                " event" : " events") + " from NDB.");

        return numEventsProcessed;
    }

    /**
     * Notify anybody listening for events that we received an event.
     */
    private void notifyEventListeners(HopsEventOperation eventOperation) {
        String eventName = eventOpToNameMapping.get(eventOperation);

        if (eventName == null) {
            LOG.error("Could not retrieve valid event name for HopsEventOperation " +
                    Integer.toHexString(eventOperation.hashCode()) + "...");
            LOG.error("Valid HopsEventOperation objects to use as keys: " +
                    StringUtils.join(eventOpToNameMapping.keySet(), ", ") + ".");
            LOG.error("Registered event names: " + StringUtils.join(eventOpToNameMapping.values(), ", "));

            for (String s : allHashCodeHexStringsEverCreated) {
                String associatedEventName = hashCodeHexStringToEventName.get(s);

                LOG.error("EventOperation '" + s + "' created for event '" + associatedEventName + "'.");
            }

            LOG.error("Returning without notifying anyone, as we cannot figure out who we're supposed to notify...");

            return;
        }

        List<HopsEventListener> eventListeners = listenersMap.get(eventName);

        LOG.debug("Notifying " + listenersMap.size() + (listenersMap.size() == 1 ? " listener " : " listeners ")
            + "of event " + eventName + ".");
        for (HopsEventListener listener : eventListeners)
            listener.eventReceived(eventOperation, eventName);
    }
}
