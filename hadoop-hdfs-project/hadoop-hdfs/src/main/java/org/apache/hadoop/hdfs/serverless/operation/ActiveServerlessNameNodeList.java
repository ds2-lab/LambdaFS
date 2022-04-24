package org.apache.hadoop.hdfs.serverless.operation;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.NuclioHandler;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * This class maintains a list of all the active serverless NameNodes. Note that the fact that a NameNode
 * is in this list does not guarantee that it is running. The list is based on the metadata available in
 * intermediate storage. It is periodically updated by the NameNode's worker thread.
 */
public class ActiveServerlessNameNodeList implements SortedActiveNodeList, Serializable {
    //public static final io.nuclio.Logger LOG = NuclioHandler.NUCLIO_LOGGER;
    public static final Logger LOG = LoggerFactory.getLogger(ActiveServerlessNameNodeList.class);
    private static final long serialVersionUID = -1602619427888192710L;

    /**
     * The active NameNodes partitioned by deployment.
     */
    private HashMap<Integer, List<ActiveNode>> activeNodesPerDeployment = new HashMap<>();

    // Initially unsorted, but gets sorted getSortedActiveNodes() gets called.
    // Becomes unsorted again once refresh() is called.
    private ArrayList<ActiveNode> activeNodes;

    /**
     * The number of deployments there are.
     */
    private int numDeployments;

    private ActiveServerlessNameNodeList() { }

    /**
     * Constructor.
     *
     * IMPORTANT: Assumes ZK group names are of the form "namenode[deploymentNumber]".
     *
     * @param zkClient ZooKeeper client. Used to establish persistent watch, so we are updated in real time about
     *                 changes in group membership.
     * @param numDeployments The total number of deployments.
     */
    public ActiveServerlessNameNodeList(ZKClient zkClient, int numDeployments) {
        this.activeNodes = new ArrayList<>();
        this.numDeployments = numDeployments;

        int deploymentNumber = 0;
        while (deploymentNumber < numDeployments) {
            final String groupName = "namenode" + deploymentNumber;
            final int currentDeployment = deploymentNumber;
            zkClient.addListener(groupName, watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        if (LOG.isDebugEnabled()) LOG.debug("Change in membership detected for deployment #" + currentDeployment + "!");
                        refreshFromZooKeeper(zkClient, currentDeployment);
                    } catch (Exception ex) {
                        LOG.error("Exception encountered while refreshing active NNs in deployment #" +
                                currentDeployment + " from ZooKeeper (in Watcher): ", ex);
                    }
                }
            });

            deploymentNumber++;
        }
    }

    public int getNumDeployments() { return this.numDeployments; }

    /**
     * Explicitly clear and refresh the active node list, using ZooKeeper as the means of tracking group membership.
     *
     * TODO: Should we add a Watcher that just calls this method as a callback to ZK events?
     *       That way, our active NN list should always be updated (for the most part).
     *
     * @param zkClient The ZooKeeper client from the {@link ServerlessNameNode} object.
     */
    public synchronized void refreshFromZooKeeper(ZKClient zkClient) throws IOException {
        int deploymentNumber = 0;
        activeNodes = new ArrayList<>();
        if (LOG.isDebugEnabled()) LOG.debug("Querying ZooKeeper for membership changes in ALL deployments.");
        while (deploymentNumber < this.numDeployments) {
            final String groupName = "namenode" + deploymentNumber;
            List<String> groupMembers = zkClient.getPermanentGroupMembers(groupName);
            List<ActiveNode> activeNodesInCurrentDeployment = new ArrayList<>(groupMembers.size());

            for (String memberId : groupMembers) {
                long id;
                try {
                    id = Long.parseLong(memberId);
                } catch (NumberFormatException ex) {
                    LOG.error("GroupMember " + memberId + " has incorrectly-formatted ID. Discarding.");
                    continue;
                }

                ActiveServerlessNameNode activeNameNode = new ActiveServerlessNameNode(id, deploymentNumber);
                activeNodes.add(activeNameNode);

                // We also keep track of the nodes per deployment.
                activeNodesInCurrentDeployment.add(activeNameNode);
            }
            activeNodesPerDeployment.put(deploymentNumber, activeNodesInCurrentDeployment);
            deploymentNumber++;
        }

        Collections.sort(activeNodes);
    }

    /**
     * Refresh the membership of the specified deployment.
     * @param zkClient ZooKeeper client.
     * @param deploymentNumber Deployment whose membership should be refreshed.
     */
    public synchronized void refreshFromZooKeeper(ZKClient zkClient, int deploymentNumber) throws Exception {
        final String groupName = "namenode" + deploymentNumber;
        List<String> groupMembers = zkClient.getPermanentGroupMembers(groupName);
        List<ActiveNode> activeNodesInSpecifiedDeployment = new ArrayList<>(groupMembers.size());
        if (LOG.isDebugEnabled()) LOG.debug("Querying ZooKeeper for membership changes in deployments #" + deploymentNumber + ".");

        for (String memberId : groupMembers) {
            long id;
            try {
                id = Long.parseLong(memberId);
            } catch (NumberFormatException ex) {
                LOG.error("GroupMember " + memberId + " has incorrectly-formatted ID. Discarding.");
                continue;
            }

            ActiveServerlessNameNode activeNameNode = new ActiveServerlessNameNode(id, deploymentNumber);

            // We also keep track of the nodes per deployment.
            activeNodesInSpecifiedDeployment.add(activeNameNode);
        }

        // Update the list of nodes for the specified deployment.
        activeNodesPerDeployment.put(deploymentNumber, activeNodesInSpecifiedDeployment);

        // Clear the current list of active nodes. Then add the list of active nodes for each deployment
        // to the master list. This seems inefficient, but the sizes of the lists should be relatively small,
        // and theoretically lots of cache hits may be fast enough.
        activeNodes.clear();
        activeNodesPerDeployment.values().forEach(activeNodes::addAll);
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

    @Override
    public ActiveNode getLeader() {
        return null;
    }

//    /**
//     * This simply returns the ActiveNode corresponding to the NameNode instance running locally.
//     */
//    @Override
//    public synchronized ActiveNode getLeader() {
//        LOG.warn("Returning local NameNode instance from call to getLeader()!");
//        return localNameNode;
//    }
}
