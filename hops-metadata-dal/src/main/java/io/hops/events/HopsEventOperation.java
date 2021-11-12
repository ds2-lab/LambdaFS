package io.hops.events;

public interface HopsEventOperation {
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
     * Return the event table of this event.
     * @return the event type of this event.
     */
    String getEventType();
}