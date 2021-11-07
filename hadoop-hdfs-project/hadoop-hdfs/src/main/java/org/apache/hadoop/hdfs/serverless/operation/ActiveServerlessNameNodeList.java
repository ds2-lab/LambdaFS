package org.apache.hadoop.hdfs.serverless.operation;

import io.hops.exception.StorageException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.ServerlessNameNodeDataAccess;
import io.hops.metadata.hdfs.entity.ServerlessNameNodeMeta;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * This class maintains a list of all the active serverless NameNodes. Note that the fact that a NameNode
 * is in this list does not guarantee that it is running. The list is based on the metadata available in
 * intermediate storage. It is periodically updated by the NameNode's worker thread.
 */
public class ActiveServerlessNameNodeList implements SortedActiveNodeList, Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(ActiveServerlessNameNodeList.class.getName());
    private static final long serialVersionUID = -1602619427888192710L;

    // Initially unsorted, but gets sorted getSortedActiveNodes() gets called.
    // Becomes unsorted again once refresh() is called.
    private final ArrayList<ActiveNode> activeNodes;

    private ActiveNode localNameNode;
    private final long localNameNodeId;

    public ActiveServerlessNameNodeList(long localNameNodeId) {
        this.activeNodes = new ArrayList<>();
        this.localNameNodeId = localNameNodeId;
    }

    /**
     * Refresh the active node list, using ZooKeeper as the means of tracking group membership.
     * @param zkClient The ZooKeeper client from the {@link ServerlessNameNode} object.
     * @param groupName The name of the group of which we are a member.
     */
    public synchronized void refreshFromZooKeeper(ZKClient zkClient, String groupName) throws Exception {
        LOG.debug("Updating the list of active NameNodes from ZooKeeper. Group name: " + groupName);

        List<String> groupMembers = zkClient.getGroupMembers(groupName);
        activeNodes.clear();

        for (String memberId : groupMembers) {
            LOG.debug("Discovered GroupMember " + memberId + ".");

            long id;
            try {
                id = Long.parseLong(memberId);
            } catch (NumberFormatException ex) {
                LOG.error("GroupMember " + memberId + " has incorrectly-formatted ID. Discarding.");
                continue;
            }

            ActiveServerlessNameNode activeNameNode = new ActiveServerlessNameNode(id);
            activeNodes.add(activeNameNode);

            if (id == localNameNodeId)
                localNameNode = activeNameNode;
        }

        if (activeNodes.size() == 1)
            LOG.debug("Finished refreshing active NameNode list. There is just one active name node now.");
        else
            LOG.debug("Finished refreshing active NameNode list. There are " + activeNodes.size() +
                    " active NameNodes now.");

        LOG.debug("Active NameNode IDs: " + activeNodes);
    }

    /**
     * Query intermediate storage for updated serverless name node metadata.
     */
    public synchronized void refreshFromStorage() throws StorageException {
        LOG.debug("Updating the list of active NameNodes from intermediate storage.");

        ServerlessNameNodeDataAccess<ServerlessNameNodeMeta> dataAccess =
                (ServerlessNameNodeDataAccess) HdfsStorageFactory.getDataAccess(ServerlessNameNodeDataAccess.class);

        List<ServerlessNameNodeMeta> allNameNodes = dataAccess.getAllServerlessNameNodes();
        activeNodes.clear();

        for (ServerlessNameNodeMeta nameNode : allNameNodes) {
            ActiveServerlessNameNode activeNameNode = new ActiveServerlessNameNode(nameNode.getNameNodeId());
            activeNodes.add(activeNameNode);

            if (nameNode.getNameNodeId() == localNameNodeId)
                localNameNode = activeNameNode;
        }

        if (activeNodes.size() == 1)
            LOG.debug("Finished refreshing active NameNode list. There is just one active name node now.");
        else
            LOG.debug("Finished refreshing active NameNode list. There are " + activeNodes.size() +
                    " active NameNodes now.");

        LOG.debug("Active NameNode IDs: " + activeNodes);
    }

    @Override
    public synchronized boolean isEmpty() {
        return activeNodes.isEmpty();
    }

    @Override
    public synchronized int size() {
        return activeNodes.size();
    }

    @Override
    public synchronized List<ActiveNode> getActiveNodes() {
        return activeNodes;
    }

    @Override
    public synchronized List<ActiveNode> getSortedActiveNodes() {
        // We lazily create the active nodes list. The list is not sorted until this function is called.
        Collections.sort(activeNodes);
        return activeNodes;
    }

    /**
     * Not supported.
     */
    @Override
    public ActiveNode getActiveNode(InetSocketAddress address) {
        throw new NotImplementedException(
                "Getting an ActiveNode by IP address is not supported for serverless name nodes.");
    }

    /**
     * This simply returns the ActiveNode corresponding to the NameNode instance running locally.
     */
    @Override
    public synchronized ActiveNode getLeader() {
        LOG.warn("Returning local NameNode instance from call to getLeader()!");
        return localNameNode;
    }
}
