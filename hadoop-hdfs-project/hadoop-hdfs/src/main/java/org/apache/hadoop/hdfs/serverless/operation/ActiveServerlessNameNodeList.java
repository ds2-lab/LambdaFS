package org.apache.hadoop.hdfs.serverless.operation;

import io.hops.exception.StorageException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.ServerlessNameNodeDataAccess;
import io.hops.metadata.hdfs.entity.ServerlessNameNodeMeta;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * This class maintains a list of all the active serverless NameNodes. Note that the fact that a NameNode
 * is in this list does not guarantee that it is running. The list is based on the metadata available in
 * intermediate storage. It is periodically updated by the NameNode's worker thread.
 */
public class ActiveServerlessNameNodeList implements SortedActiveNodeList {
    public static final Logger LOG = LoggerFactory.getLogger(ActiveServerlessNameNodeList.class.getName());

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
     * Query intermediate storage for updated serverless name node metadata.
     */
    public synchronized void refresh() throws StorageException {
        LOG.debug("Updating the list of active NameNodes.");

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
