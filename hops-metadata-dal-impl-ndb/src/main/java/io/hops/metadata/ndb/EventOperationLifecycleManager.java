package io.hops.metadata.ndb;

import com.mysql.clusterj.core.store.EventOperation;
import io.hops.events.HopsEventOperation;
import io.hops.metadata.ndb.wrapper.HopsEventOperationImpl;

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
    private final ConcurrentHashMap<EventOperation, HopsEventOperation> eventOperationHashMap =
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
     * Return an existing HopsEventOperation instance created for the given ClusterJ/C++ object, if one exists.
     * Otherwise, we create a new HopsEventOperation instance and associate it with the ClusterJ/C++ object, so we
     * can return the same HopsEventOperation in the future.
     *
     * @param eventOperation The {@link EventOperation} object returned by
     * {@link io.hops.metadata.ndb.wrapper.HopsSession#nextEvent(String)}.
     *
     * @return An existing HopsEventOperation if possible, otherwise a new HopsEventOperation. In either case, the
     * HopsEventOperation is wrapping the given EventOperation object.
     */
    public HopsEventOperation getOrCreateInstance(String eventName, EventOperation eventOperation) {
        // If there is no existing HopsEventOperationImpl instance associated with the given ClusterJ
        // EventOperation instance, then we create a new HopsEventOperationImpl, persist the new mapping in the
        // HashMap, and return the newly-created HopsEventOperationImpl instance. This all happens atomically.
        return eventOperationHashMap.computeIfAbsent(eventOperation,
                eventOp -> new HopsEventOperationImpl(eventOp, eventName));
    }
}
