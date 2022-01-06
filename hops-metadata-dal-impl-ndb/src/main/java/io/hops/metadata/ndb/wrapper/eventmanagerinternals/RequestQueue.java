package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RequestQueue<T> {
    /**
     * Queue of requests. This maintains order.
     */
    private BlockingQueue<T> queue;

    /**
     * Mapping of event name to the associated request. This maintains indexability.
     */
    private ConcurrentHashMap<String, T> map;

    private final int queueCapacity;

    /**
     * Create a new RequestQueue.
     *
     * @param queueCapacity The maximum capacity of the request queue.
     */
    public RequestQueue(int queueCapacity) {
        this.queueCapacity = queueCapacity;

        this.queue = new ArrayBlockingQueue<T>(queueCapacity);
        this.map = new ConcurrentHashMap<>();
    }

    ////////////////
    // PUBLIC API //
    ////////////////

    public synchronized void add(String eventName, T request) {
        this.queue.add(request);
        this.map.put(eventName, request);
    }

    /**
     * Return the request associated with the given event name if one exists, otherwise null.
     * @param eventName The name of the event in question.
     * @return The request associated with the given event, if one such request exists. Otherwise, null.
     */
    public synchronized T get(String eventName) {
        return this.map.getOrDefault(eventName, null);
    }

    /**
     * Remove and return the request at the head of the queue.
     * This prevents this request from being indexed in the future.
     *
     * @return The next request to process.
     */
    public synchronized T take() {
        T request = queue.poll();

        if (request == null)
            return null;

        return request;
    }

    public int getQueueCapacity() { return this.queueCapacity; }

    /////////////////
    // PRIVATE API //
    /////////////////
}
