package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

public class HopsEventOperation {
    static final Log LOG = LogFactory.getLog(HopsEventOperation.class);

    /**
     * We need to maintain an instance to this class in order to perform the necessary operations.
     */
    private final EventOperation clusterJEventOperation;

    /**
     * Mapping from column name to the RecordAttribute associated with the pre-event value of the given column.
     */
    private final HashMap<String, RecordAttr> preValueRecordAttributes;

    /**
     * Mapping from column name to the RecordAttribute associated with the post-event value of the given column.
     */
    private final HashMap<String, RecordAttr> postValueRecordAttributes;

    /**
     * The name of the event for which this event operation was created.
     *
     * In some cases, this will be null or a meaningless value, like "N/A". Consequently, this
     * variable should only be used for debugging/logging, and nothing more.
     */
    private final String associatedEventName;

    public HopsEventOperation(EventOperation clusterJEventOperation, String eventName) {
        this.clusterJEventOperation = clusterJEventOperation;
        this.preValueRecordAttributes = new HashMap<>();
        this.postValueRecordAttributes = new HashMap<>();
        this.associatedEventName = eventName;
    }

    /**
     * We can compare equality of the wrapped ClusterJ event operation objects.
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HopsEventOperation))
            return false;

        HopsEventOperation otherEventOperation = (HopsEventOperation)other;

        return this.clusterJEventOperation.equals(otherEventOperation.clusterJEventOperation);
    }

    @Override
    public int hashCode() {
        return this.clusterJEventOperation.hashCode();
    }

    /**
     * Return the event name associated with this event operation, which could be null or "N/A"
     * if the name of the associated event was not available when this event operation object
     * was created (which is not uncommon).
     */
    public String getEventName() {
        return associatedEventName;
    }

    /**
     * Get the 32-bit int pre-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The 32-bit int pre-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such pre-record attribute for the specified column.
     */
    public int getIntPreValue(String columnName) throws IllegalArgumentException {
        if (!preValueRecordAttributes.containsKey(columnName))
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);

        return preValueRecordAttributes.get(columnName).int32_value();
    }

    /**
     * Get the 32-bit int post-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The 32-bit int post-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such post-record attribute for the specified column.
     */
    public int getIntPostValue(String columnName) throws IllegalArgumentException {
        if (!postValueRecordAttributes.containsKey(columnName))
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);

        return postValueRecordAttributes.get(columnName).int32_value();
    }

    /**
     * Get the long pre-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The long value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such pre-record attribute for the specified column.
     */
    public long getLongPreValue(String columnName) throws IllegalArgumentException {
        if (!preValueRecordAttributes.containsKey(columnName))
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);

        return preValueRecordAttributes.get(columnName).int64_value();
    }

    /**
     * Get the long post-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The long value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such post-record attribute for the specified column.
     */
    public long getLongPostValue(String columnName) throws IllegalArgumentException {
        if (!postValueRecordAttributes.containsKey(columnName))
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);

        return postValueRecordAttributes.get(columnName).int64_value();
    }

    /**
     * Add a pre- and post-value record attribute for the given column name to this event operation.
     *
     * @param columnName The name of the column for which a record attribute will be created.
     *
     * @return True if at least one of the pre- and post-value record attributes were created, otherwise false.
     * False may be returned if both record attribute already exist. Generally either both should exist or neither
     * should exist.
     *
     * If an error occurs while creating the record attribute, then an exception will be thrown.
     */
    public boolean addRecordAttribute(String columnName) {
        boolean preCreated = false;
        boolean postCreated = false;

        if (!preValueRecordAttributes.containsKey(columnName)) {
            LOG.debug("Created pre-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
            RecordAttr preAttribute = clusterJEventOperation.getPreValue(columnName);
            preValueRecordAttributes.put(columnName, preAttribute);
            preCreated = true;
        }

        if (!postValueRecordAttributes.containsKey(columnName)) {
            LOG.debug("Created post-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
            RecordAttr postAttribute = clusterJEventOperation.getValue(columnName);
            postValueRecordAttributes.put(columnName, postAttribute);
            postCreated = true;
        }

        // If at least one of these was created, then we'll return true.
        return (preCreated || postCreated);
    }

    /**
     * Remove a pre- and post-value record attribute for the given column name to this event operation.
     *
     * @param columnName The name of the column for which a record attribute will be removed.
     *
     * @return True if the record attribute was removed, otherwise false. False may be returned if the
     * specified record attribute does not exist.
     */
    public boolean removeRecordAttribute(String columnName) {
        boolean preRemoved = false;
        boolean postRemoved = false;

        if (preValueRecordAttributes.containsKey(columnName)) {
            LOG.debug("Removed pre-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
            preValueRecordAttributes.remove(columnName);
            preRemoved = true;
        }

        if (postValueRecordAttributes.containsKey(columnName)) {
            LOG.debug("Removed post-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
            postValueRecordAttributes.remove(columnName);
            postRemoved = true;
        }

        // If at least one of these was created, then we'll return true.
        return (preRemoved || postRemoved);
    }

    /**
     * Return the event table of this event.
     * @return the event type of this event.
     */
    public TableEvent getEventType() {
        return clusterJEventOperation.getEventType();
    }

    /**
     * Return the underlying/wrapped ClusterJ EventOperation. This is package private as the ClusterJ EventOperation
     * object should only be interfaced with by this library. It is abstracted away for clients of this library.
     * @return the underlying/wrapped ClusterJ EventOperation.
     */
    protected EventOperation getClusterJEventOperation() {
        return clusterJEventOperation;
    }
}
