package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;

public class OutgoingRequest {
    public final int targetDeployment;
    public final JsonObject arguments;
    public final String functionUri;
    public final String authorizationString;

    public OutgoingRequest(int targetDeployment, JsonObject arguments, String functionUri, String authorizationString) {
        this.targetDeployment = targetDeployment;
        this.arguments = arguments;
        this.functionUri = functionUri;
        this.authorizationString = authorizationString;
    }
}
