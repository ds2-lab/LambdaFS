package io.hops.events;

/**
 *  CREATED = 0,
 *  EXECUTING = 1,
 *  DROPPED = 2,
 *  ERROR = 3
 */
public enum HopsEventState {
    CREATED,
    EXECUTING,
    DROPPED,
    ERROR;

    /**
     * Convert the integer ID to the HopsEventState enum value.
     */
    public static HopsEventState convert(int id) {
        switch (id) {
            case 0:
                return CREATED;
            case 1:
                return EXECUTING;
            case 2:
                return DROPPED;
            case 3:
                return ERROR;
            default:
                throw new IllegalArgumentException("The only valid integer values are 0, 1, 2, or 3, not " + id);
        }
    }
}
