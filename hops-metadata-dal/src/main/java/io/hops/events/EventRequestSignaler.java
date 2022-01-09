package io.hops.events;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Used to signal to Threads that an event (and optionally an event subscription) has been registered with
 * intermediate storage. This is used during the consistency protocol, as Threads cannot proceed with the protocol
 * until subscriptions are established. (Once subscriptions are established, the Thread carrying out the protocol
 * can receive ACKs from other NameNodes. Failing to subscribe first could cause ACKs to be missed.)
 */
public class EventRequestSignaler {
    /**
     * If True, then this barrier should not allow threads to pass until both the event has been created AND
     * the subscription has been created.
     */
    private final boolean requireSubscription;

    /**
     * Indicates whether the event has been created.
     */
    private volatile boolean eventCreated = false;

    /**
     * Indicates whether the event subscription has been created.
     */
    private volatile boolean subscriptionCreated = false;

    /**
     * Used to create the condition object and also synchronize access to this object.
     */
    private final Lock lock;

    /**
     * Threads block on this condition if they call acquire() before the event (and possibly the subscription)
     * have been created.
     */
    private final Condition condition;

    public EventRequestSignaler(boolean requireSubscription) {
        this.requireSubscription = requireSubscription;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    ////////////////
    // PUBLIC API //
    ////////////////

    /**
     * Returns true if the creation of a subscription on the associated event is also required for this signaler
     * to allow threads to pass through.
     */
    public boolean isSubscriptionRequired() { return requireSubscription; }

    /**
     * Called by the {@link EventManager} when associated the event has been created.
     * Checks if this barrier can begin permitting threads to continue executing.
     */
    public void eventCreated() {
        this.eventCreated = true;
        checkIfCriteriaSatisfied();
    }

    /**
     * Called by the {@link EventManager} when the associated subscription has been created.
     * Checks if this barrier can begin permitting threads to continue executing.
     */
    public void subscriptionCreated() {
        this.subscriptionCreated = true;
        checkIfCriteriaSatisfied();
    }

    /**
     * Wait until all criteria have been satisfied.
     */
    public void acquire() throws InterruptedException {
        lock.lock();
        try {
            while (!criteriaSatisfied())
                condition.await();
        } finally {
            lock.unlock();
        }
    }

    public boolean criteriaSatisfied() {
        return (!requireSubscription && eventCreated) || (requireSubscription && eventCreated && subscriptionCreated);
    }

    /////////////////
    // PRIVATE API //
    /////////////////

    /**
     * Check if the event and subscription (if required, otherwise just the event) have been created.
     * If so, signal this to all waiting threads.
     */
    public void checkIfCriteriaSatisfied() {
        lock.lock();
        try {
            if (criteriaSatisfied()) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }
}
