package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import io.hops.events.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.eventmanagerinternals.EventRequest;
import io.hops.events.EventRequestSignaler;
import io.hops.metadata.ndb.wrapper.eventmanagerinternals.RequestQueue;
import io.hops.metadata.ndb.wrapper.eventmanagerinternals.ScheduledSubscriptionRemoval;
import io.hops.metadata.ndb.wrapper.eventmanagerinternals.SubscriptionRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
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
    private static final int POLL_TIME_MILLISECONDS = 300;

    /**
     * The capacity of the BlockingQueues in which requests to create/drop event subscriptions are placed.
     */
    private static final int REQUEST_QUEUE_CAPACITIES = 50;

    /**
     * The maximum number of requests to create a new event subscription that we'll process before stopping to
     * listen for more events.
     */
    private static final int MAX_SUBSCRIPTION_REQUESTS = REQUEST_QUEUE_CAPACITIES;

    /**
     * Amount of time, in milliseconds, that we wait before dropping an event subscription after it has been
     * scheduled for removal.
     */
    private static final int DROP_SUBSCRIPTION_WAIT_INTERVAL_MILLISECONDS = 60000;

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
     * Base name (i.e., name without the integer at the end) for the invalidations tables.
     */
    private static final String INV_TABLE_NAME_BASE = "invalidations_deployment";

    /**
     * Internal queue to keep track of requests to create/register events.
     */
    private final RequestQueue<EventRequest> createEventQueue;

    private final ConcurrentHashMap<String, EventRequestSignaler> eventCreationSignalers;

    /**
     * Internal queue used to keep track of requests to create event subscriptions that failed the first time.
     */
    private final RequestQueue<SubscriptionRequest> createSubscriptionRetryQueue;

    /**
     * These are used exclusively for resubmitted/failed event subscriptions.
     */
    private final ConcurrentHashMap<String, EventRequestSignaler> subscriptionCreationSignalers;

    /**
     * Internal queue used to keep track of requests to drop event subscriptions.
     */
    private final BlockingQueue<SubscriptionRequest> dropSubscriptionQueue;

    /**
     * Map from event name to ScheduledSubscriptionRemoval instances. Basically allows us to check if a given
     * event subscription is actively scheduled for removal at the present moment.
     */
    private final ConcurrentHashMap<String, ScheduledSubscriptionRemoval> scheduledSubscriptionRemovalMap;

    /**
     * Priority queue maintaining the drop-subscription requests that are actively scheduled.
     */
    private final PriorityQueue<ScheduledSubscriptionRemoval> scheduledSubscriptionRemovals;

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
     * These are the events that all NameNodes subscribe to for ACK events. We only want to receive
     * notifications about updates, as updating an existing entry is now NameNodes perform an acknowledgement.
     *
     * We do not want to receive notifications when they are added or deleted.
     */
    private static final Integer[] ACK_TABLE_EVENTS = new Integer[] {
            TableEvent.convert(TableEvent.UPDATE)
    };

    /**
     * Invalidation events should only notify us on an insertion. They are never updated,
     * and deleting them as part of clean-up is not something we need to be informed of.
     */
    private static final Integer[] INV_TABLE_EVENTS = new Integer[] {
            TableEvent.convert(TableEvent.INSERT)
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
        this.createSubscriptionRetryQueue = new RequestQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.dropSubscriptionQueue = new ArrayBlockingQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.subscriptionCreationSignalers = new ConcurrentHashMap<>();
        this.createEventQueue = new RequestQueue<>(REQUEST_QUEUE_CAPACITIES);
        this.eventCreationSignalers = new ConcurrentHashMap<>();
        this.scheduledSubscriptionRemovalMap = new ConcurrentHashMap<>();
        this.scheduledSubscriptionRemovals = new PriorityQueue<>();
    }

    ////////////////
    // PUBLIC API //
    ////////////////

    /**
     * Register an event listener.
     *
     * @param listener the event listener to be registered.
     * @param eventName the name of the event for which we're registering an event listener.
     */
    public void addListener(String eventName, HopsEventListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("The event listener cannot be null.");

        LOG.debug("Adding event listener for event '" + eventName + "'.");

        List<HopsEventListener> eventListeners;
        if (this.listenersMap.containsKey(eventName)) {
            eventListeners = this.listenersMap.get(eventName);
        } else {
            eventListeners = new ArrayList<>();
            this.listenersMap.put(eventName, eventListeners);
        }

        eventListeners.add(listener);
        // updateCount(eventName, true);

        LOG.debug("Registered new event listener. Number of listeners: " + listenersMap.size() + ".");
    }

    @Override
    public void requestDropSubscription(String eventName, HopsEventListener eventListener)
            throws StorageException {
        LOG.debug("Issuing request to drop subscription for event " + eventName + " now...");

        SubscriptionRequest dropRequest = new SubscriptionRequest(eventName, eventListener, new String[0],
                SubscriptionRequest.SubscriptionOperation.DROP_SUBSCRIPTION);

        try {
            dropSubscriptionQueue.put(dropRequest);
        } catch (IllegalStateException | InterruptedException e) {
            throw new StorageException("Failed to enqueue request to drop subscription for event " +
                    eventName + " due to exception:", e);
        }
    }

    @Override
    public Integer[] getAckEventTypeIDs() {
        return ACK_TABLE_EVENTS;
    }

    @Override
    public String[] getAckTableEventColumns() {
        return ACK_EVENT_COLUMNS;
    }

    @Override
    public EventRequestSignaler requestCreateEvent(String eventName, String tableName, String[] eventColumns,
                                        boolean recreateIfExists, boolean alsoCreateSubscription,
                                        HopsEventListener eventListener, Integer[] tableEvents) throws StorageException {
        // Make sure that the parameters passed are valid.
        if (!alsoCreateSubscription && eventListener != null) {
            LOG.error("Indicated that no subscription should be created for event " + eventName +
                    ", but also provided an event listener...");
            throw new IllegalStateException("Event Listener should be null when 'alsoCreateSubscription' is false.");
        }

        SubscriptionRequest subscriptionTask = null;
        if (alsoCreateSubscription) {
            subscriptionTask = new SubscriptionRequest(eventName, eventListener, eventColumns,
                    SubscriptionRequest.SubscriptionOperation.CREATE_SUBSCRIPTION);
        }

        // The 'subscriptionTask' parameter will be null if `alsoCreateSubscription` is false.
        EventRequest eventRegistrationRequest =
                new EventRequest(eventName, tableName, eventColumns, recreateIfExists, subscriptionTask, tableEvents);

        try {
            createEventQueue.put(eventRegistrationRequest);
        } catch (IllegalStateException  e) {
            throw new StorageException("Failed to enqueue request to drop subscription for event " +
                    eventName + " due to IllegalStateException :", e);
        }

        // Create a new EventRequestSignaler and store it in the mapping.
        EventRequestSignaler eventRequestSignaler = new EventRequestSignaler(alsoCreateSubscription);
        eventCreationSignalers.put(eventRegistrationRequest.getRequestId(), eventRequestSignaler);

        return eventRequestSignaler;
    }

    @Override
    public void waitUntilSetupDone() throws InterruptedException {
        defaultSetupSemaphore.acquire();
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

        // Loop forever, listening for events.
        while (true) {
            try {
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
                    int numEventsProcessed = processEvents();
                    LOG.debug("Processed " + numEventsProcessed + " event(s).");
                }

                processEventRegistrationRequests();      // Process requests to create/register new events.
                processFailedEventCreationRequests();    // Process requests to create subscriptions.
                processDropSubscriptionRequests();       // Process requests to drop subscriptions.
                processScheduledSubscriptionRemovals();  // Process scheduled subscription removals.
            }
            catch (Exception ex) {
                LOG.error("EventManager encountered an exception:", ex);
            }
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

    /////////////////
    // PRIVATE API //
    /////////////////
    private HopsSession obtainSession() {
        return this.session;
    }

    /**
     * Unregister an event listener.
     *
     * TODO: Consider calling this in {@link HopsEventManager#requestDropSubscription(String, HopsEventListener)},
     *       as it might be more efficient to remove the listener immediately. Doing so would require additional
     *       concurrency control, however.
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
                throw new IllegalArgumentException("The provided event listener is not registered with " + eventName + "!");

            eventListeners.remove(listener);
        } else {
            throw new IllegalArgumentException("We have no event listeners registered for event " + eventName + "!");
        }
    }

    private static final List<String> allHashCodeHexStringsEverCreated = new ArrayList<String>();
    private static final Map<String, String> hashCodeHexStringToEventName = new HashMap<>();
    // private static Lock _mutex = new ReentrantLock();


//    @Override
//    public EventRequestSignaler requestCreateSubscriptionWithListener(String eventName, HopsEventListener listener)
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
//        EventRequestSignaler creationNotifier = new EventRequestSignaler(0);
//        subscriptionCreationNotifiers.put(createSubscriptionRequest, creationNotifier);
//
//        return creationNotifier;
//    }

    /**
     * Enqueue an event subscription creation task for retry after it failed to be created the first time.
     *
     * TODO: Update this to new API semantics.
     */
    private EventRequestSignaler resubmitSubscriptionCreationRequest(SubscriptionRequest failedTask) throws StorageException {
        LOG.debug("Enqueuing failed event subscription creation task for event " + failedTask.getEventName() +
                " for retry. Number of failed attempts: " + failedTask.getNumFailedAttempts());

        failedTask.incrementFailedAttempts();

        try {/**/
            createSubscriptionRetryQueue.put(failedTask);
        } catch (IllegalStateException e) {
            throw new StorageException("Failed to enqueue request to create subscription for event " +
                    failedTask.getEventName() + " due to IllegalStateException:", e);
        }

        EventRequestSignaler creationNotifier = new EventRequestSignaler(true);
        subscriptionCreationSignalers.put(failedTask.getEventName(), creationNotifier);

        return creationNotifier;
    }

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

        LOG.debug("Successfully created subscription on event '" + eventName + "': " +
                Integer.toHexString(hopsEventOperation.hashCode()));

        eventOperationMap.put(eventName, hopsEventOperation);
        eventOpToNameMapping.put(hopsEventOperation, eventName);

        if (eventName.contains(HopsEvent.ACK_EVENT_NAME_BASE))
            eventColumnMap.put(hopsEventOperation, ACK_EVENT_COLUMNS);
        else if (eventName.contains(HopsEvent.INV_EVENT_NAME_BASE))
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
     * Actually unregister the event operation with NDB. This doesn't do any checks for active event listeners.
     * @param eventName Name associated with the event operation that is being unregistered.
     * @param hopsEventOperation The event operation that we're dropping.
     */
    private void unregisterEventOperationUnsafe(String eventName, HopsEventOperation hopsEventOperation)
            throws StorageException {
        // Make sure to remove the event from the eventMap.
        eventOperationMap.remove(eventName);
        eventOpToNameMapping.remove(hopsEventOperation);

        // Try to drop the event. If something goes wrong, we'll throw an exception.
        boolean dropped;
        try {
            dropped = obtainSession().dropEventOperation(hopsEventOperation, eventName);
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

    /**
     * Schedule a subscription to be formally dropped. This can only occur if there are no active event listeners
     * on the subscription. If there is at least one active listener, then we will not drop the subscription.
     *
     * This function WILL remove the listener of the caller. If there are no more listeners, then this function will
     * schedule the subscription to be dropped after a fixed period of time. The subscription will eventually be dropped
     * if there are no new, active listeners added or present when the fixed period of time elapses.
     *
     * This should only be used internally, for the same reasons that the createEventOperation function is only used
     * internally.
     *
     * @param eventName Name associated with the event operation that is being unregistered.
     * @param eventListener EventListener associated with the event we're removing.
     */
    private void scheduleSubscriptionRemoval(String eventName, HopsEventListener eventListener) throws StorageException {
        LOG.debug("Unregistering EventOperation for event " + eventName + " with NDB now...");

        // If we aren't tracking an event with the given name, then the argument is invalid.
        if (!eventOperationMap.containsKey(eventName))
            throw new IllegalArgumentException("There is no event operation associated with an event called "
                    + eventName + " currently being tracked by the EventManager.");

        // First, unregister the event listener, as we do that no matter what.
        if (eventListener != null)
            removeListener(eventName, eventListener);

        List<HopsEventListener> listeners = listenersMap.get(eventName);
        int currentNumberOfListeners = (listeners == null ? 0 : listeners.size());
        if (currentNumberOfListeners > 0) {
            LOG.debug("There are still " + currentNumberOfListeners + " event listener(s) for event " +
                    eventName + ". Will not drop the subscription.");
        } else {
            LOG.debug("There are no more event listeners associated with the event. " +
                    "Scheduling the subscription for removal now.");

            ScheduledSubscriptionRemoval scheduledSubscriptionRemoval =
                    new ScheduledSubscriptionRemoval(DROP_SUBSCRIPTION_WAIT_INTERVAL_MILLISECONDS, eventName);
            scheduledSubscriptionRemovals.add(scheduledSubscriptionRemoval);
            scheduledSubscriptionRemovalMap.put(eventName, scheduledSubscriptionRemoval);
        }

        // unregisterEventOperationUnsafe(eventName, hopsEventOperation);
    }

    /**
     * Register an event with intermediate storage. This will not do anything if we already have a reference
     * to an event with the same name AND `recreateIfExists` is false.
     *
     * @param eventName The name of the event.
     * @param tableName The table on which the event will be registered.
     * @param eventColumns The columns of the table that will be involved with the event.
     * @param recreateIfExists If true, recreate the event if it already exists.
     * @param tableEvents The event types that we want to receive for the given subscription (e.g., update,
     *                    insert, delete, etc.).
     */
    private void registerEvent(String eventName, String tableName, String[] eventColumns,
                               boolean recreateIfExists, TableEvent[] tableEvents) throws StorageException {
        LOG.debug("Registering event " + eventName + " on table " + tableName + " with NDB now...");

        // If we're already tracking this event, and we aren't supposed to recreate it, then just return.
        if (eventMap.containsKey(eventName)) {
            if (!recreateIfExists) {
                LOG.debug("Event '" + eventName + "' is already being tracked by the EventManager.");
                return;
            } else {
                LOG.debug("Event '" + eventName + "' already exists, but we've been instructed to recreate it.");
            }
        }

        // Try to create the event. If something goes wrong, we'll throw an exception.
        Event event;
        try {
            event = obtainSession().createAndRegisterEvent(eventName, tableName, eventColumns,
                    tableEvents, recreateIfExists);
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
     * Register the event encapsulated by the given {@link EventRequest} instance.
     * @param eventTask Represents the event to be created.
     */
    private void registerEvent(EventRequest eventTask) throws StorageException {
        String eventName = eventTask.getEventName();
        String tableName = eventTask.getTableName();
        String[] eventColumns =  eventTask.getEventColumns();
        boolean recreateIfExists = eventTask.isRecreateIfExists();
        TableEvent[] tableEvents = eventTask.getTableEvents();

        registerEvent(eventName, tableName, eventColumns, recreateIfExists, tableEvents);
    }

    /**
     * Process any requests we've received to register events with NDB. Note that these requests may contain
     * subscription-creation requests, which will also be processed by this function.
     */
    private void processEventRegistrationRequests() {
        int requestsProcessed = 0;
        while (createEventQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            EventRequest eventRegistrationRequest = createEventQueue.poll();

            if (eventRegistrationRequest == null)
                throw new IllegalStateException("Size of createEventQueue is " + createEventQueue.size() +
                        ", but poll() returned null...");

            LOG.debug("Processing event creation request for event " + eventRegistrationRequest.getEventName());
            try {
                registerEvent(eventRegistrationRequest);
            } catch (StorageException e) {
                LOG.error("Failed to register event " + eventRegistrationRequest.getEventName() + ": ", e);
            }

            requestsProcessed++;
            String eventName = eventRegistrationRequest.getEventName();
            EventRequestSignaler eventRegisteredNotifier =
                    eventCreationSignalers.get(eventRegistrationRequest.getRequestId());

            if (eventRegisteredNotifier == null)
                throw new IllegalStateException("We do not have an subscription creation notifier for event " + eventName + "!");

            // If the EventTask has an included SubscriptionTask, then the EventRequestSignaler would've been initialized
            // with -1 permits, so the other thread would still be blocked if it called .acquire() on the EventRequestSignaler.
            eventRegisteredNotifier.eventCreated();

            SubscriptionRequest subscriptionTask = eventRegistrationRequest.getSubscriptionTask();
            if (subscriptionTask != null)
                processCreateSubscriptionTask(subscriptionTask, eventRegisteredNotifier);
        }
    }

    /**
     * Process a SubscriptionTask to create a subscription for an existing event.
     *
     * IMPORTANT: This calls release(1) on the EventRequestSignaler associated with the SubscriptionTask instance.
     *
     * @param createSubscriptionRequest The task that we're processing.
     * @param signaler The EventRequestSignaler for this subscription. It's the same EventRequestSignaler used for the event itself.
     */
    private void processCreateSubscriptionTask(SubscriptionRequest createSubscriptionRequest,
                                               EventRequestSignaler signaler) {
        String eventName = createSubscriptionRequest.getEventName();

        // If we get null, then just exit.
        if (eventName == null) {
            LOG.error("Event name for SubscriptionRequest " + createSubscriptionRequest.getRequestId() +
                    " is null. Cannot process request.");
            return;
        }

        // Make sure we have a notifier.
        if (signaler == null)
            throw new IllegalStateException("We do not have an subscription creation notifier for event " +
                    eventName + "!");

        LOG.debug("Processing subscription creation request for event " + eventName);
        HopsEventListener eventListener = createSubscriptionRequest.getEventListener();

        HopsEventOperation eventOperation = null;
        try {
            eventOperation = createAndReturnEventOperation(eventName);
        } catch (StorageException e) {
            LOG.error("Encountered exception while trying to create event subscription for event " + eventName + ":", e);
        }

        // Make sure we were successful in creating this event operation.
        if (eventOperation == null)
            throw new IllegalStateException("Failed to register event subscription with NDB for event '" + eventName + "'");

        // Add record attributes for each of the specified columns. We use these to get MySQL table column values.
        String[] eventColumns = createSubscriptionRequest.getEventColumns();
        for (String columnName : eventColumns)
            eventOperation.addRecordAttribute(columnName);

        // If we have a listener to add, we add it before executing.
        if (eventListener != null) {
            addListener(eventName, eventListener);
        } else {
            LOG.warn("Creating subscription on event '" + eventName + "', but event listener is null...");
        }

        // If the event is in the CREATED state, then it was freshly created just now (we didn't already have an
        // existing subscription for the event), in which case we need to call execute().
        if (eventOperation.getState() == HopsEventState.CREATED) {
            LOG.debug("Executing event operation for event '" + eventName + "'");
            eventOperation.execute();
        } else if (eventOperation.getState() == HopsEventState.EXECUTING) {
            LOG.debug("Not calling .execute() on event subscription for event " + eventName +
                    ". Subscription is already active.");
        }

        // TODO: Update this code to new API semantics, specifically the resubmitSubscriptionCreationRequest()
        //       function and how it handles/creates the signaler object.
//        // Check for error state. If there is an error state, resubmit the event operation for reattempt.
//        if (eventOperation.getState() == HopsEventState.ERROR) {
//            LOG.error("The event subscription for event " + eventName +
//                    " has not entered the EXECUTING state. It is in state " + eventOperation.getState());
//
//            // We might miss some events because of this...
//            int numExistingListeners = this.listenersMap.get(eventName).size();
//            if (numExistingListeners > 0) {
//                LOG.error("There are already listeners " + numExistingListeners +
//                        " registered with failed event operation " + eventName + "!");
//            }
//
//            try {
//                unregisterEventOperationUnsafe(eventName, eventOperation);
//            } catch (StorageException e) {
//                LOG.error("Failed to unregister erred event subscription for event " + eventName + ": ", e);
//            }
//
//            try {
//                resubmitSubscriptionCreationRequest(createSubscriptionRequest);
//            } catch (StorageException e) {
//                LOG.error("Failed to resubmit event subscription creation task for event " +
//                        eventName + ": ", e);
//            }
//
//            return;
//        }

        // Make sure we're in the executing state before we call .release() on the EventRequestSignaler.
        HopsEventState currentState = eventOperation.getState();
        if (currentState != HopsEventState.EXECUTING) {
            throw new IllegalStateException("Event subscription for event " + eventName +
                    " is in unexpected state: " + currentState);
        }

        // We call the final release here as we need to call .execute() on the event operation first.
        // Release two, covering both cases (listener required or no listener required).
        signaler.subscriptionCreated();

        // Attempt to cancel a scheduled subscription removal for this event, seeing as we just created a new
        // subscription. It is the case that, if we tried to cancel the subscription while the newest event listener
        // was active, then we'd see the event listener and abort the cancellation; however, if this newly-added
        // listener is removed before the scheduled subscription is supposed to be removed, then we would not see
        // the listener, and we would remove the subscription. This should not occur, as the fact that we've used the
        // subscription again should refresh the removal, and the wait interval should begin when the last listener
        // is removed.
        cancelScheduledSubscriptionRemoval(eventName);
    }

    /**
     * Attempt to cancel the scheduled removal of a particular event subscription. If there is no such scheduled
     * removal for the indicated event, then this function does nothing.
     *
     * @param eventName The name of the event for which a subscription removal may be scheduled.
     */
    private void cancelScheduledSubscriptionRemoval(String eventName) {
        if (scheduledSubscriptionRemovalMap.containsKey(eventName)) {
            ScheduledSubscriptionRemoval scheduledSubscriptionRemoval = scheduledSubscriptionRemovalMap.get(eventName);
            scheduledSubscriptionRemovals.remove(scheduledSubscriptionRemoval);
            scheduledSubscriptionRemovalMap.remove(eventName);

            LOG.debug("Cancelled scheduled subscription removal for event '" + eventName +
                    "'. Subscription would have been dropped in approximately " +
                    scheduledSubscriptionRemoval.timeUntilRemoval() + " ms.");
        }
    }

    /**
     * Try to create event subscription tasks that previously failed.
     */
    private void processFailedEventCreationRequests() {
        int requestsProcessed = 0;
        while (createSubscriptionRetryQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            SubscriptionRequest failedSubscriptionCreationTask = createSubscriptionRetryQueue.poll();
            String eventName = failedSubscriptionCreationTask.getEventName();

            LOG.debug("Retrying failed event subscription creation task for event " + eventName);

            assert(failedSubscriptionCreationTask.getSubscriptionOperation() ==
                    SubscriptionRequest.SubscriptionOperation.CREATE_SUBSCRIPTION);

            // If we get null, then just exit.
            if (eventName == null)
                break;

            EventRequestSignaler signaler = subscriptionCreationSignalers.get(eventName);

            if (signaler == null)
                throw new IllegalStateException("We do not have an subscription creation notifier for event " +
                        eventName + "!");

            processCreateSubscriptionTask(failedSubscriptionCreationTask, signaler);
            requestsProcessed++;
        }
    }

    /**
     * Check if there are any subscriptions that are ready to be actively, fully dropped.
     */
    private void processScheduledSubscriptionRemovals() {
        LOG.debug("Processing scheduled event subscription removals now.");
        int numProcessed = 0;

        // We drop ALL that are ready, regardless of how many there are.
        while (scheduledSubscriptionRemovals.size() > 0) {
            // Don't remove it yet.
            ScheduledSubscriptionRemoval tmp = scheduledSubscriptionRemovals.peek();

            // Check if it is time to drop this subscription yet.
            if (tmp.shouldDrop()) {
                String eventName = tmp.getEventName();
                List<HopsEventListener> listeners = listenersMap.get(eventName);

                // Make sure nobody is using this subscription now, and nobody has issued a request for this subscription.
                if (listeners != null && listeners.size() > 0) {
                    // In this scenario, there are now some listeners for the event we were going to drop.
                    // So, we shouldn't drop it at this point. We should just eliminate the scheduling removal.
                    LOG.debug("Cannot drop subscription on event '" + eventName + "'. Number of listeners: " + listeners.size());
                }
                else if (createEventQueue.contains(eventName)) {
                    // In this scenario, there is a request to create a subscription on this event already in the
                    // queue, so we shouldn't drop the subscription now. It will just get recreated when that
                    // queued creation request gets processed.
                    LOG.debug("Found a event/subscription creation request in the queue for event '" + eventName +
                            "'. Opting to NOT drop the subscription, since it will just get recreated.");
                }
                else {
                    // Call poll() here so that it actually gets removed from the priority queue.
                    ScheduledSubscriptionRemoval toRemove = scheduledSubscriptionRemovals.poll();
                    assert(toRemove == tmp); // They should be the exact same object.

                    // There are still no active listeners, so we can drop the subscription.
                    HopsEventOperation hopsEventOperation = eventOperationMap.get(eventName);

                    LOG.debug("Dropping subscription on event '" + eventName + "'.");

                    try {
                        unregisterEventOperationUnsafe(eventName, hopsEventOperation);
                        numProcessed++;
                    } catch (StorageException ex) {
                        LOG.error("Failed to drop subscription on event '" + eventName + "'.");
                        // TODO: Handle a failure here.
                    }
                }

                // Now remove it from the map. Whether we dropped it or not doesn't matter; we should remove it.
                scheduledSubscriptionRemovalMap.remove(eventName);
            } else {
                // Since we're iterating over a priority queue, if we encounter a scheduled subscription that
                // isn't ready yet, then none of the subsequent subscriptions will be ready,
                // so we can just break out of the loop now.
                break;
            }
        }

        if (numProcessed > 0)
            LOG.debug("Successfully dropped " + numProcessed + " scheduled subscription(s).");
    }

    /**
     * Process any requests we've received to drop event subscriptions.
     */
    private void processDropSubscriptionRequests() {
        int requestsProcessed = 0;
        while (dropSubscriptionQueue.size() > 0 && requestsProcessed < MAX_SUBSCRIPTION_REQUESTS) {
            SubscriptionRequest dropSubscriptionRequest = dropSubscriptionQueue.poll();
            String eventName = dropSubscriptionRequest.getEventName();
            HopsEventListener eventListener = dropSubscriptionRequest.getEventListener();

            assert(dropSubscriptionRequest.getSubscriptionOperation() ==
                    SubscriptionRequest.SubscriptionOperation.DROP_SUBSCRIPTION);

            // If we get null, then just exit.
            if (eventName == null) {
                LOG.error("Event name for drop-subscription request " + dropSubscriptionRequest.getRequestId() +
                        " is null. Cannot process request.");
                requestsProcessed++;
                continue;
            }

            try {
                scheduleSubscriptionRemoval(eventName, eventListener);
            } catch (StorageException e) {
                LOG.error("Encountered exception while trying to create event subscription for event " +
                        eventName + ":", e);
            }

            requestsProcessed++;
        }
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
        //
        // We convert the integer representation of the event types for invalidation events as that last
        // parameter. Specifically, we're using Java 8 streams to convert each integer ID to the corresponding
        // ClusterJ TableEvent enum.
        registerEvent(defaultEventName, tableName, INV_TABLE_EVENT_COLUMNS, defaultDeleteIfExists,
                Arrays.stream(INV_TABLE_EVENTS).map(TableEvent::convert).toArray(TableEvent[]::new));

        LOG.debug("Creating event operation for event " + defaultEventName + " now...");
        HopsEventOperation eventOperation = createAndReturnEventOperation(defaultEventName);

        LOG.debug("Setting up record attributes for event " + defaultEventName + " now...");
        for (String columnName : INV_TABLE_EVENT_COLUMNS)
            eventOperation.addRecordAttribute(columnName);

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
    private int processEvents() {
        HopsEventOperation nextEventOp = session.nextEvent();
        int numEventsProcessed = 0;

        while (nextEventOp != null) {
            String eventType = nextEventOp.getEventType();

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
            nextEventOp = session.nextEvent();
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
        // LOG.debug("Notifying " + eventListeners.size() + (eventListeners.size() == 1 ? " listener " : " listeners ") + "of event " + eventName + ".");
        for (HopsEventListener listener : eventListeners)
            listener.eventReceived(eventOperation, eventName);
    }
}
