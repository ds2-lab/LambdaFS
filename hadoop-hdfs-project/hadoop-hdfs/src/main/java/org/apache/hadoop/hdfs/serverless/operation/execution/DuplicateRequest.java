package org.apache.hadoop.hdfs.serverless.operation.execution;

import java.io.Serializable;

/**
 * Used to indicate that the request was a duplicate and therefore no result will be returned.
 *
 * The NameNode-side code adds an extra field to the JSON payload returned to the client if it sees an instance
 * of this class being used as a result. This extra field indicates that the particular payload is for a duplicate
 * request and is not the result of the file system operation. (The extra field is also included if the result is
 * NOT an instance of this class; in that case, the field is just set to 'false' rather than 'true'.)
 *
 * TODO: Do we need a whole class for this? Can't there just be a field/flag?
 *       Not particularly urgent because duplicate requests are quite rare, but still.
 */
public class DuplicateRequest implements Serializable {

    private static final long serialVersionUID = 7192661459086515520L;

    /**
     * Should be HTTP or TCP.
     */
    private String requestType;

    /**
     * Unique ID of the file system task associated with this DuplicateRequest object.
     *
     * Recall that the IDs of tasks are just the request IDs of the associated HTTP/TCP request(s).
     */
    private String taskId;

    private DuplicateRequest() { }

    public DuplicateRequest(String requestType, String taskId) {
        this.requestType = requestType;
        this.taskId = taskId;
    }

    public String getRequestType() {
        return requestType;
    }

    public String getTaskId() {
        return taskId;
    }
}