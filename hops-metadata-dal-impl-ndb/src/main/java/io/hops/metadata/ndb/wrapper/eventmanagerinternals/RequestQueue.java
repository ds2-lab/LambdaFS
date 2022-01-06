package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RequestQueue<T extends EventRequestBase> {
    /**
     * Queue of requests. This maintains order.
     */
    private final BlockingQueue<T> queue;

    /**
     * Mapping of event name to the associated request. This maintains indexability.
     */
    private final ConcurrentHashMap<String, T> requestMap;

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
     * If a request for this event already exists, then this does NOT add the request to the queue, nor is a mapping
     * created (as one already exists in this case).
     *
     * @param request The request to add to the queue.
     * @return True if there does not already exist a request associated with this event (and thus the given request
     * was added to the queue). Otherwise, returns false.
     */
    public synchronized boolean put(T request) throws InterruptedException {
        String eventName = request.getEventName();

        if (this.requestMap.containsKey(eventName))
            return false;

        this.queue.put(request);
        this.requestMap.put(eventName, request);

        return true;
    }

    /**
     * Returns true if there is a request associated with the specified event. Otherwise, returns false.
     * @param eventName The name of the event in question.
     */
    public synchronized boolean contains(String eventName) { return this.requestMap.containsKey(eventName); }

    /**
     * Return the request associated with the given event name if one exists, otherwise null.
     * This does NOT remove the requested element. Therefore, this function should NOT be used when actually
     * processing requests.
     *
     * @param eventName The name of the event in question.
     * @return The request associated with the given event, if one such request exists. Otherwise, null.
     */
    public synchronized T get(String eventName) {
        return this.requestMap.getOrDefault(eventName, null);
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
        requestMap.remove(eventName);               // ...and then we remove the request from the map.

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
