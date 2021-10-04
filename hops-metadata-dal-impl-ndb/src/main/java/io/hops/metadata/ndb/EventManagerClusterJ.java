package io.hops.metadata.ndb;

import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import io.hops.EventManager;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.security.auth.login.Configuration;
import java.util.HashMap;

/**
 * This class is responsible for listening to events from NDB and reacting to them appropriately.
 *
 * The events serve as cache invalidations for NameNodes. The NameNodes cache metadata locally in-memory. An Event
 * from NDB on the table for which the NameNode caches data serves to inform the NameNode that its cache is now
 * out-of-date.
 */
public class EventManagerClusterJ implements EventManager {
    static final Log LOG = LogFactory.getLog(EventManager.class);

    /**
     * These are the events that all NameNodes subscribe to.
     */
    private static final TableEvent[] eventsToSubscribeTo = new TableEvent[] {
            TableEvent.INSERT,
            TableEvent.DELETE,
            TableEvent.UPDATE
    };

    /**
     * All registered events are contained in here.
     */
    private HashMap<String, Event> eventMap;

    /**
     * All active EventOperation instances are contained in here.
     */
    private HashMap<String, EventOperation> eventOperationMap;

    /**
     * The active session with the database. Used to issue operations related to events,
     * and to receive events from the database.
     */
    private HopsSession session;

    public EventManagerClusterJ(HopsSession session) {
        this.session = session;
    }

    public EventManagerClusterJ() throws StorageException {
        this.session = DBSessionProvider.sessionFactory.getSession();
    }

    /**
     * Create and register an event with the given name.
     * @param eventName Unique identifier of the event to be created.
     * @param recreateIfExisting If true, delete and recreate the event if it already exists.
     * @return True if the event was created and registered successfully, otherwise false.
     */
    @Override
    public boolean registerEvent(String eventName, String tableName, boolean recreateIfExisting)
            throws StorageException {

        Event event = session.createAndRegisterEvent(eventName, tableName, eventsToSubscribeTo);

        return false;
    }

    /**
     * Delete the event with the given name.
     * @param eventName Unique identifier of the event to be deleted.
     * @return True if an event with the given name was deleted, otherwise false.
     */
    @Override
    public boolean unregisterEvent(String eventName) throws StorageException {
        session.even
        return false;
    }

    @Override
    public void run() {
        LOG.debug("The EventManager has started running.");

        // Loop forever, listening for events.
        while (true) {

        }
    }
}
