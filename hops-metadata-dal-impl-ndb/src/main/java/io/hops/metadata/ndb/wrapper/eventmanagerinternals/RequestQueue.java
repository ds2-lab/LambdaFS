package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RequestQueue<T extends EventRequestBase> {
    /**
     * Queue of requests. This maintains order.
     */
    private final BlockingQueue<T> queue;

    /**
     * Mapping of event name to the associated request. This is just used to determine if the queue currently
     * contains a request for a particular event. Since each event can have multiple subscriptions, this
     * maps to a list of requests.
     */
    private final ConcurrentHashMap<String, List<T>> requestMap;

    private final int queueCapacity;

    /**
     * Create a new RequestQueue.
     *
     * @param queueCapacity The maximum capacity of the request queue.
     */
    public RequestQueue(int queueCapacity) {
        this.queueCapacity = queueCapacity;

        this.queue = new ArrayBlockingQueue<T>(queueCapacity);
        this.requestMap = new ConcurrentHashMap<>();
    }

    ////////////////
    // PUBLIC API //
    ////////////////

    /**
     * Add the given request to the queue. This also creates a mapping from the given event name to the given request.
     *
     * @param request The request to add to the queue.
     */
    public synchronized void put(T request) throws IllegalStateException  {
        String eventName = request.getEventName();

        this.queue.add(request);

        List<T> requests = requestMap.getOrDefault(eventName, new ArrayList<>());
        requests.add(request);
        this.requestMap.put(eventName, requests);
    }

    /**
     * Returns true if there is a request associated with the specified event. Otherwise, returns false.
     * @param eventName The name of the event in question.
     */
    public synchronized boolean contains(String eventName) {
        List<T> requests = this.requestMap.get(eventName);

        // If this list doesn't even exist, then return false.
        if (requests == null)
            return false;

        // If there is at least one request in this list, then return true.
        return requests.size() > 0;
    }

    /**
     * Remove and return the request at the head of the queue.
     * This prevents this request from being indexed in the future.
     *
     * This will NOT block if the queue is empty. Instead, this function will just return null.
     *
     * @return The next request to process if there is such a request. Otherwise, returns null.
     */
    public synchronized T poll() {
        T request = queue.poll();   // Attempt to remove a request from the queue.

        if (request == null)        // If the request was null, then we return null.
            return null;

        String eventName = request.getEventName();  // Otherwise, we first grab the name of the associated event...

        List<T> requests = requestMap.get(eventName);
        requests.remove(request);                   // ...and then we remove the request from the map.

        assertSizeConsistency();

        return request; // Finally, we return the next request to be processed.
    }

    /**
     * Return the number of requests in the queue. (This is the same as the number of requests in the map.)
     */
    public int size() {
        assertSizeConsistency();
        return this.queue.size();
    }

    public int getQueueCapacity() { return this.queueCapacity; }

    /////////////////
    // PRIVATE API //
    /////////////////

    /**
     * Assert the size of the queue is equal to the size of the map.
     */
    private void assertSizeConsistency() {
        assert(this.queue.size() == this.requestMap.size());
    }
}
