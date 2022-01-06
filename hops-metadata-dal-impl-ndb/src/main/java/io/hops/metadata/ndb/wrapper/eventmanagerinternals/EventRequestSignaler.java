package io.hops.metadata.ndb.wrapper.eventmanagerinternals;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EventRequestSignaler {
    /**
     * If True, then this barrier should not allow threads to pass until both the event has been created AND
     * the subscription has been created.
     */
    private final boolean requireSubscription;

    private boolean eventCreated = false;
    private boolean subscriptionCreated = false;

    private final Lock lock;
    private final Condition condition;

    public EventRequestSignaler(boolean requireSubscription) {
        this.requireSubscription = requireSubscription;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    ////////////////
    // PUBLIC API //
    ////////////////

    public boolean isRequireSubscription() { return requireSubscription; }

    /**
     * Called by the {@link io.hops.metadata.ndb.wrapper.HopsEventManager} when associated the event has been created.
     * Checks if this barrier can begin permitting threads to continue executing.
     */
    public void eventCreated() {
        this.eventCreated = true;
        checkIfCriteriaSatisfied();
    }

    /**
     * Called by the {@link io.hops.metadata.ndb.wrapper.HopsEventManager} when the associated subscription has been created.
     * Checks if this barrier can begin permitting threads to continue executing.
     */
    public void subscriptionCreated() {
        this.subscriptionCreated = true;
        checkIfCriteriaSatisfied();
    }

    /**
     * Wait until all criteria have been satisfied.
     */
    public void waitForCriteria() throws InterruptedException {
        lock.lock();
        try {
            while (!criteriaSatisfied())
                condition.wait();
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
