package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

/**
 * Encapsulates an event subscription that needs to be dropped.
 *
 * Another design option would be to use a ScheduledExecutorService, but I don't want to worry about multi-threaded
 * synchronization for this. Plus, the thread that created the subscription needs to be the one that drops it.
 */
public class ScheduledSubscriptionRemoval implements Comparable<ScheduledSubscriptionRemoval> {
    /**
     * The timestamp at which we scheduled this subscription to be dropped.
     */
    private final long scheduledAt;

    /**
     * How long to wait before actually dropping the subscription.
     */
    private final long waitInterval;

    /**
     * The time at/after which we can actually drop the subscription.
     */
    private final long dropAfterTime;

    /**
     * The name of the event associated with the subscription to be dropped.
     */
    private final String eventName;

    public ScheduledSubscriptionRemoval(long dropAfterMilliseconds, String eventName) {
        if (eventName == null)
            throw new IllegalArgumentException("The eventName parameter cannot be null.");

        if (dropAfterMilliseconds < 0)
            throw new IllegalArgumentException("The dropAfterMilliseconds parameter must be non-negative.");

        this.scheduledAt = System.currentTimeMillis();
        this.waitInterval = dropAfterMilliseconds;
        this.dropAfterTime = this.scheduledAt + this.waitInterval;
        this.eventName = eventName;
    }

    /**
     * Returns true if the required time has elapsed and the subscription can be dropped. Otherwise, returns false.
     */
    public boolean shouldDrop() {
        return System.currentTimeMillis() > this.dropAfterTime;
    }

    public long getWaitInterval() {
        return waitInterval;
    }

    public long getScheduledAt() {
        return scheduledAt;
    }

    public String getEventName() {
        return eventName;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ScheduledSubscriptionRemoval))
            return false;

        ScheduledSubscriptionRemoval other = (ScheduledSubscriptionRemoval)obj;

        return this.eventName.equals(other.eventName);
    }

    @Override
    public int hashCode() {
        return eventName.hashCode();
    }

    @Override
    public int compareTo(ScheduledSubscriptionRemoval o) {
        return Long.compare(this.dropAfterTime, o.dropAfterTime);
    }
}
