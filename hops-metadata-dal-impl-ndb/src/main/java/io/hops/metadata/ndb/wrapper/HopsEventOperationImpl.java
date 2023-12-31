package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import io.hops.events.HopsEventOperation;
import io.hops.events.HopsEventState;
import io.hops.metadata.ndb.NdbBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

public class HopsEventOperationImpl implements HopsEventOperation {
    static final Log LOG = LogFactory.getLog(HopsEventOperationImpl.class);

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

    public HopsEventOperationImpl(EventOperation clusterJEventOperation, String eventName) {
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
        if (!(other instanceof HopsEventOperationImpl))
            return false;

        HopsEventOperationImpl otherEventOperation = (HopsEventOperationImpl)other;

        return this.clusterJEventOperation.equals(otherEventOperation.clusterJEventOperation);
    }

    @Override
    public int hashCode() {
//        LOG.debug("Returning hashCode() for underlying clusterJEventOperation " +
//                Integer.toHexString(clusterJEventOperation.hashCode()) + ".");
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

    @Override
    public boolean getBooleanPreValue(String columnName) {
        if (!preValueRecordAttributes.containsKey(columnName))  {
            LOG.error("Failed to find desired pre-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid pre-value record attributes include: " +
                    StringUtils.join(preValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);
        }

        return NdbBoolean.convert(preValueRecordAttributes.get(columnName).int8_value());
    }

    @Override
    public boolean getBooleanPostValue(String columnName) {
        if (!postValueRecordAttributes.containsKey(columnName)) {
            LOG.error("Failed to find specified post-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid post-value record attributes include: " +
                    StringUtils.join(postValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No post-value record attribute exists for column " + columnName);
        }
        return NdbBoolean.convert(postValueRecordAttributes.get(columnName).int8_value());
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
    @Override
    public int getIntPreValue(String columnName) throws IllegalArgumentException {
        if (!preValueRecordAttributes.containsKey(columnName)) {
            LOG.error("Failed to find desired pre-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid pre-value record attributes include: " +
                    StringUtils.join(preValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);
        }

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
    @Override
    public int getIntPostValue(String columnName) throws IllegalArgumentException {
        if (!postValueRecordAttributes.containsKey(columnName)) {
            LOG.error("Failed to find specified post-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid post-value record attributes include: " +
                    StringUtils.join(postValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No post-value record attribute exists for column " + columnName);
        }

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
    @Override
    public long getLongPreValue(String columnName) throws IllegalArgumentException {
        if (!preValueRecordAttributes.containsKey(columnName)) {
            LOG.error("Failed to find desired pre-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid pre-value record attributes include: " +
                    StringUtils.join(preValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No pre-value record attribute exists for column " + columnName);
        }

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
    @Override
    public long getLongPostValue(String columnName) throws IllegalArgumentException {
        if (!postValueRecordAttributes.containsKey(columnName)) {
            LOG.error("Failed to find specified post-value record attribute '" + columnName +
                    "' for event operation " + Integer.toHexString(this.hashCode()) + ".");
            LOG.error("Valid post-value record attributes include: " +
                    StringUtils.join(postValueRecordAttributes.keySet(), ", "));
            throw new IllegalArgumentException("No post-value record attribute exists for column " + columnName);
        }

        return postValueRecordAttributes.get(columnName).int64_value();
    }

    @Override
    public void execute() {
        this.clusterJEventOperation.execute();
    }

    @Override
    public void addRecordAttribute(String columnName) {
        //boolean preCreated = false;
        //boolean postCreated = false;

        if (!preValueRecordAttributes.containsKey(columnName)) {
            RecordAttr preAttribute = clusterJEventOperation.getPreValue(columnName);
            preValueRecordAttributes.put(columnName, preAttribute);
            //preCreated = true;
            LOG.debug("Created pre-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
        }

        if (!postValueRecordAttributes.containsKey(columnName)) {
            RecordAttr postAttribute = clusterJEventOperation.getValue(columnName);
            postValueRecordAttributes.put(columnName, postAttribute);
            //postCreated = true;
            LOG.debug("Created post-value record attribute for column " + columnName +
                    " for event " + associatedEventName + ".");
        }

        // If at least one of these was created, then we'll return true.
        //return (preCreated || postCreated);
    }

    @Override
    public HopsEventState getState() {
        return HopsEventState.convert(clusterJEventOperation.getState());
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
    @Override
    public String getEventType() {
        return clusterJEventOperation.getEventType().name();
    }

    /**
     * For internal use only; get the ClusterJ TableEvent object indicating what type of event this is.
     */
    protected TableEvent getUnderlyingEventType() {
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
