package io.hops.events;

public interface HopsEventOperation {
    /**
     * Add a pre- and post-value record attribute for the given column name to this event operation.
     *
     * @param columnName The name of the column for which a record attribute will be created.
     *
     * Previously, we would return a boolean indicating success. But this boolean really served no purpose.
     * If an error occurs while creating the record attribute, then an exception will be thrown.
     * And now that event operations can be re-used, we may end up not creating any NdbRecAttr objects
     * simply because they already exist.
     */
    void addRecordAttribute(String columnName);

    /**
     * Get the boolean pre-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The boolean pre-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such pre-record attribute for the specified column.
     */
    boolean getBooleanPreValue(String columnName);


    /**
     * Get the boolean post-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The boolean post-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such post-record attribute for the specified column.
     */
    boolean getBooleanPostValue(String columnName);

    /**
     * Get the 32-bit int pre-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The 32-bit int pre-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such pre-record attribute for the specified column.
     */
    int getIntPreValue(String columnName) throws IllegalArgumentException;

    /**
     * Get the 32-bit int post-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The 32-bit int post-value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such post-record attribute for the specified column.
     */
    int getIntPostValue(String columnName) throws IllegalArgumentException;

    /**
     * Get the long pre-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The long value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such pre-record attribute for the specified column.
     */
    long getLongPreValue(String columnName) throws IllegalArgumentException;

    /**
     * Get the long post-value of a record attribute (i.e., value associated with an event).
     *
     * @param columnName The name of the column whose value is desired.
     *
     * @return The long value of the desired record attribute.
     *
     * @throws IllegalArgumentException If there is no such post-record attribute for the specified column.
     */
    long getLongPostValue(String columnName) throws IllegalArgumentException;

    /**
     * Activates the NdbEventOperation to start receiving events. The
     * changed attribute values may be retrieved after Ndb::nextEvent()
     * has returned not NULL. The getValue() methods must be called
     * prior to execute().
     */
    void execute();

    /**
     * Return the event table of this event.
     * @return the event type of this event.
     */
    String getEventType();
}
