package org.apache.hadoop.hdfs.serverless.consistency;

import io.hops.leader_election.node.ActiveNode;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Basically just holds onto the ID. All the other fields are meaningless for serverless name nodes.
 */
public class ActiveServerlessNameNode implements ActiveNode, Serializable {
    private static final long serialVersionUID = 646982592726977047L;
    private long id;
    private int deploymentNumber;

    private ActiveServerlessNameNode() { }

    public ActiveServerlessNameNode(long id) {
        this.id = id;
        this.deploymentNumber = -1;
    }

    public ActiveServerlessNameNode(long id, int deploymentNumber) {
        this.id = id;
        this.deploymentNumber = deploymentNumber;
    }

    public int getDeploymentNumber() { return this.deploymentNumber; }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getRpcServerIpAddress() {
        return null;
    }

    @Override
    public int getRpcServerPort() {
        return -1;
    }

    @Override
    public InetSocketAddress getRpcServerAddressForClients() {
        return null;
    }

    @Override
    public String getServiceRpcIpAddress() {
        return null;
    }

    @Override
    public int getServiceRpcPort() {
        return -1;
    }

    @Override
    public InetSocketAddress getRpcServerAddressForDatanodes() {
        return null;
    }

    @Override
    public String getHttpAddress() {
        return null;
    }

    @Override
    public int getLocationDomainId() {
        return -1;
    }

    @Override
    public String toString() {
        return "ActiveServerlessNN(ID=" + id + ", Deployment#=" + deploymentNumber + ")";
    }

    @Override
    public int compareTo(ActiveNode o) {
        if (this.getId() < o.getId()) {
            return -1;
        } else if (this.getId() == o.getId()) {
            return 0;
        } else {
            return 1;
        }
    }
}
