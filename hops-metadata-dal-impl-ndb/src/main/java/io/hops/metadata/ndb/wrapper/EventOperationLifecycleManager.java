package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.core.store.EventOperation;
import io.hops.events.HopsEventOperation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages HopsEventOperation (really, HopsEventOperationImpl) objects.
 *
 * The NDB C++ library reuses the *same* C++ NDBEventOperation objects each time you call nextEvent(). That is,
 * the same object that gets returned when you call createEventOperation() is reused (and therefore returned) in
 * calls to nextEvent(). Everytime that event operation receives a new event, the same exact instance
 * NdbEventOperation instance is returned with new data inside it.
 *
 * The Java-level HopsEventOperationImpl objects do wrap the underlying C++ objects. So, when we call nextEvent(),
 * we return an instance of HopsEventOperationImpl that wraps the same C++ NdbEventOperation everytime. But previously,
 * we called 'new HopsEventOperationImpl()' everytime we called nextEvent(). This would effectively erase the
 * Java-level state we maintained on the HopsEventOperationImpl instance (e.g., record attribute mappings, which are
 * used to retrieve column values from an event).
 *
 * This class enables us to mimic the same pattern used in the C++ library. That is, the same HopsEventOperationImpl
 * instance will be returned each time the C++ level returns a given NDBEventOperation.
 *
 * The {@link io.hops.metadata.ndb.wrapper.HopsSession} class interfaces with us to get/create HopsEventOperationImpl
 * instances via our createInstance() function.
 *
 * Follows a singleton pattern.
 */
public class EventOperationLifecycleManager {
    private static final Log LOG = LogFactory.getLog(EventOperationLifecycleManager.class);

    /**
     * We map native C++ NdbEventOperation objects to their corresponding HopsEventOperationImpl instances.
     *
     * In general, we shouldn't have too many HopsEventOperationImpl instances at any given time. There will be
     * spikes during write operations, though. So to account for this, we set the initial capacity to 20. Aside from
     * writes, there's really just the one invalidation event.
     *
     * The main thread, worker thread, event manager thread, and consistency protocol thread may access this map,
     * so we set the concurrency level to four.
     */
//    private final ConcurrentHashMap<EventOperation, HopsEventOperation> eventOperationHashMap =
//            new ConcurrentHashMap<>(20, 0.75f /* default */, 4);

    /**
     * Mapping from ClusterJ event operation objects to HopsFS event operation objects. This is used when
     * calling {@link HopsSession#nextEvent()}, rather than when creating new subscriptions.
     */
    private final ConcurrentHashMap<EventOperation, HopsEventOperation> eventOperationHashMap1 =
            new ConcurrentHashMap<>(20, 0.75f /* default */, 4);

    /**
     * Mapping from event names to the HopsEventOperation objects associated with them. This mapping is used
     * when creating new event subscriptions (i.e., when calling {@link HopsSession#createEventOperation(String)}).
     */
    private final ConcurrentHashMap<String, HopsEventOperation> eventOperationHashMap2 =
            new ConcurrentHashMap<>(20, 0.75f /* default */, 4);

    /**
     * Default constructor. Private so only we can create an instance.
     */
    private EventOperationLifecycleManager() {

    }

    /**
     * Singleton instance. We use a singleton as we only want one instance of the eventOperationHashMap.
     */
    private static EventOperationLifecycleManager instance;

    public static EventOperationLifecycleManager getInstance() {
        // Class-level lock to ensure only a single instance of EventLifecycleManager is ever created.
        synchronized (EventOperationLifecycleManager.class) {
            if (instance == null)
                instance = new EventOperationLifecycleManager();
        }

        return instance;
    }

    /**
     *
     * @param eventOperation The underlying ClusterJ event operation that is being dropped.
     * @param eventName The name of the event for which the subscription is being dropped.
     */
    public synchronized void deleteInstance(EventOperation eventOperation, String eventName) {
        eventOperationHashMap1.remove(eventOperation);
        eventOperationHashMap2.remove(eventName);
    }

    /**
     * Return an existing HopsEventOperation instance created for the given ClusterJ/C++ object, if one exists.
     * Otherwise, we create a new HopsEventOperation instance and associate it with the ClusterJ/C++ object, so we
     * can return the same HopsEventOperation in the future.
     *
     * We use this overload with {@link HopsSession#nextEvent()}, NOT when creating/registering a new event
     * subscription.
     *
     * @param eventOperation The {@link EventOperation} object returned by
     * {@link io.hops.metadata.ndb.wrapper.HopsSession#nextEvent()}.
     *
     * @return An existing HopsEventOperation if possible, otherwise a new HopsEventOperation. In either case, the
     * HopsEventOperation is wrapping the given EventOperation object.
     */
    public synchronized HopsEventOperation getExistingInstance(EventOperation eventOperation) {
        return eventOperationHashMap1.get(eventOperation);
    }

    /**
     * We use this overload when creating/registering new event subscriptions, not when calling
     * {@link HopsSession#nextEvent()}.
     *
     * @param eventName The name of the event for which a subscription should be created.
     * @param session ClusterJ session object in case we actually need to create a new subscription object.
     *
     * @return An existing HopsEventOperation if possible, otherwise a new HopsEventOperation. In either case, the
     * HopsEventOperation is wrapping the given EventOperation object.
     */
    public synchronized HopsEventOperation getOrCreateInstance(String eventName, Session session) {
        // Check if we've already created an event operation for the specified event. If not,
        // then we'll create a new one.
        HopsEventOperation hopsEventOperation;

        // It's possible a subscription already exists for this event, in which case we'll return it.
        if (eventOperationHashMap2.containsKey(eventName))
            hopsEventOperation = eventOperationHashMap2.get(eventName);
        else { // Otherwise, we have to create the new subscription ourselves.
            EventOperation eventOperation = session.createEventOperation(eventName);
            hopsEventOperation = new HopsEventOperationImpl(eventOperation, eventName);
            eventOperationHashMap2.put(eventName, hopsEventOperation);

            // The mapping in the other HashMap should NOT already exist.
            if (eventOperationHashMap1.containsKey(eventOperation))
                throw new IllegalStateException("Recreated existing ClusterJ event operation for event " + eventName);

            eventOperationHashMap1.put(eventOperation, hopsEventOperation);
        }

        return hopsEventOperation;
    }
}
