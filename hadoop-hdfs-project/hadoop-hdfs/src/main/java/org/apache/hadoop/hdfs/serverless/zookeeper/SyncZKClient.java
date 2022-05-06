package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.serverless.invoking.InvokerUtilities;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * Encapsulates ZooKeeper/Apache Curator Framework functionality for the NameNode.
 */
public class SyncZKClient implements ZKClient {
    public static final Log LOG = LogFactory.getLog(SyncZKClient.class);

    /**
     * Directory for permanent members of a deployment. The full path would be:
     * /namenode[deployment_number][PERMANENT_DIR].
     */
    public static final String PERMANENT_DIR = "/permanent";

    /**
     * Directory for guest members of a deployment. The full path would be:
     * /namenode[deployment_number][GUEST_DIR].
     */
    public static final String GUEST_DIR = "/guest";

    /**
     * Encapsulates a connection to the ZooKeeper ensemble.
     */
    private CuratorFramework client;

    /**
     * The hostnames to try connecting to.
     */
    private final String[] hosts;

    /**
     * The connection string used to connect to the ZooKeeper ensemble. Constructed from the
     * {@link SyncZKClient#hosts} instance variable.
     */
    private final String connectionString;

    /**
     * Keep track of all our watchers. Generally there should just be one (for the group we're in).
     */
    private final ConcurrentHashMap<String, PersistentWatcher> watchers;

    /**
     * How long for ZooKeeper to consider a NameNode to have disconnected (in milliseconds).
     *
     * This should be LESS than the transaction acknowledgement phase timeout so that NNs have enough time
     * to detect ZooKeeper membership changes.
     */
    private final int sessionTimeoutMilliseconds;

    /**
     * How long for a connection attempt to the ZooKeeper ensemble to timeout (in milliseconds).
     */
    private final int connectionTimeoutMilliseconds;

    /**
     * Constructor.
     * @param conf HopsFS Configuration used to configure the ZooKeeper client.
     */
    public SyncZKClient(Configuration conf) {
        if (conf == null)
            throw new IllegalArgumentException("The 'conf' array argument must be non-null.");

        this.hosts = conf.getStrings(SERVERLESS_ZOOKEEPER_HOSTNAMES, SERVERLESS_ZOOKEEPER_HOSTNAMES_DEFAULT);
        this.connectionTimeoutMilliseconds = conf.getInt(SERVERLESS_ZOOKEEPER_CONNECT_TIMEOUT,
                SERVERLESS_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
        this.sessionTimeoutMilliseconds = conf.getInt(SERVERLESS_ZOOKEEPER_SESSION_TIMEOUT,
                SERVERLESS_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);

        if (hosts.length == 0)
            throw new IllegalArgumentException("The ZooKeeper 'hosts' configuration parameter must specify at least " +
                    "one ZK server! (Currently, it is empty.)");

        StringBuilder connectionStringBuilder = new StringBuilder();
        for (int i = 0; i < this.hosts.length; i++) {
            String host = this.hosts[i];

            connectionStringBuilder.append(host);

            if (i < this.hosts.length - 1)
                connectionStringBuilder.append(',');
        }
        this.connectionString = connectionStringBuilder.toString();
        this.watchers = new ConcurrentHashMap<>();
    }

    /**
     * Return a flag indicating whether we're currently connected to ZooKeeper.
     */
    public boolean verifyConnection() {
        return this.client.getZookeeperClient().isConnected();
    }

    /**
     * Connect to the ZooKeeper ensemble.
     *
     * @return A {@link ZooKeeper object} representing the connection to the server/ensemble.
     */
    private CuratorFramework connectToZooKeeper() {
        // These are reasonable arguments for the ExponentialBackoffRetry. The first
        // retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        // TODO: Make these configurable?
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 8);

        return CuratorFrameworkFactory.newClient(connectionString, sessionTimeoutMilliseconds,
                connectionTimeoutMilliseconds, retryPolicy);
    }

    @Override
    public void connect() {
        LOG.debug("Connecting to the ZK ensemble now...");
        this.client = connectToZooKeeper();
        LOG.debug("Connected successfully to ZK ensemble. Starting ZK client now...");
        this.client.start();
        LOG.debug("Successfully started ZK client.");
    }

    @Override
    public void createGroup(String groupName) throws Exception {
        if (this.client == null)
            throw new IllegalStateException("ZooKeeper client must be instantiated before joining a group.");

        if (groupName == null)
            throw new IllegalArgumentException("Group name parameter must be non-null.");

        // Permanent refers to the fact that this is the path for permanent group members.
        String permanentPath = getPath(groupName, null, true);
        String guestPath = getPath(groupName, null, false);
        String invPath = getInvPath(groupName);

        try {
            LOG.debug("Creating ZK group with path: " + permanentPath);
            // This will throw an exception if the group already exists!
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(permanentPath);
        } catch (KeeperException.NodeExistsException ex) {
            LOG.debug("ZooKeeper group '" + permanentPath + "' already exists.");
        }

        try {
            LOG.debug("Creating ZK group with path: " + guestPath);
            // This will throw an exception if the group already exists!
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(guestPath);
        } catch (KeeperException.NodeExistsException ex) {
            LOG.debug("ZooKeeper group '" + guestPath + "' already exists.");
        }

        try {
            LOG.debug("Creating ZK group with path: " + invPath);
            // This will throw an exception if the group already exists!
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(invPath);
        } catch (KeeperException.NodeExistsException ex) {
            LOG.debug("ZooKeeper group '" + invPath + "' already exists.");
        }
    }

    @Override
    public void joinGroupAsGuest(String groupName, String memberId, Watcher watcher,
                                 GuestWatcherOption watcherOption) throws Exception {
        if (this.client == null)
            throw new IllegalStateException("ZooKeeper client must be instantiated before joining a group.");

        if (groupName == null)
            throw new IllegalArgumentException("Group name parameter must be non-null.");

        String path = getPath(groupName, memberId, false);

        LOG.debug("Joining ZK (guest) group with path: " + path);
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path);

        if (watcherOption == GuestWatcherOption.CREATE_WATCH_ON_PERMANENT) {
            LOG.debug("Creating watcher on permanent sub-group for group " + groupName + ".");
            addListener(groupName, watcher);
        }
        else if (watcherOption == GuestWatcherOption.CREATE_WATCH_ON_GUEST) {
            LOG.debug("Creating watcher on guest sub-group for group " + groupName + ".");
            addGuestListener(groupName, watcher);
        }
        else if (watcherOption == GuestWatcherOption.CREATE_WATCH_ON_BOTH) {
            LOG.debug("Creating watcher on both the permanent and guest sub-groups for group " + groupName + ".");
            addListener(groupName, watcher);
            addGuestListener(groupName, watcher);
        }
        else {
            LOG.warn("Skipping creation of PersistentWatcher instance for group " + groupName + ".");
        }
    }

    @Override
    public void joinGroupAsPermanent(String groupName, String memberId,
                                     Invalidatable invalidatable, boolean createWatch) throws Exception {
        if (this.client == null)
            throw new IllegalStateException("ZooKeeper client must be instantiated before joining a group.");

        if (groupName == null)
            throw new IllegalArgumentException("Group name parameter must be non-null.");

        String path = getPath(groupName, memberId, true);

        LOG.debug("Joining ZK (permanent) group with path: " + path);
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path);

        String parentPath = getPath(groupName, null, true);
        if (createWatch) {
            LOG.debug("Creating PersistentWatcher for parent path '" + parentPath + "'");
            createStateChangedWatcher(invalidatable);
        } else {
            LOG.warn("Skipping creation of PersistentWatcher for parent path '" + parentPath + "'");
        }
    }

    /**
     * Create and start a PersistentWatcher for the given path.
     *
     * @param path The path for which to create the PersistentWatcher.
     * @param recursive Passed to the PersistentWatcher constructor.
     *
     * @return the created PersistentWatcher instance. Note that the instance will already be started (i.e., it will
     * have had .start() called on it).
     */
    private PersistentWatcher getOrCreatePersistentWatcher(String path, boolean recursive) {
        return watchers.computeIfAbsent(path, p -> {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating new PersistentWatcher for path '" + p + '"');
            PersistentWatcher persistentWatcher = new PersistentWatcher(this.client, p, recursive);
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully created PersistentWatcher for path '" + p + "'. Starting watch now.");
            persistentWatcher.start();
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully started and started PersistentWatcher for path '" + p + "'.");
            return persistentWatcher;
        });
    }

    /**
     * Create and start a PersistentWatcher instance for the given path.
     *
     * @param invalidatable Used as a callback for state changes detected by the PersistentWatcher.
     *                      If null, then no callback occurs, but a message is logged.
     */
    private void createStateChangedWatcher(Invalidatable invalidatable) {
        // We need to invalidate our cache whenever our connection to ZooKeeper is lost.
        client.getConnectionStateListenable().addListener((curatorFramework, connectionState) -> {
            if (!connectionState.isConnected()) {
                LOG.warn("Connection to ZooKeeper lost. Need to invalidate cache.");
                if (invalidatable != null)
                    invalidatable.invalidateCache();
                else
                    LOG.warn("Provided invalidatable object was null!");
            } else {
                LOG.debug("Connected established with ZooKeeper ensemble.");
                // TODO: If our connection is automatically re-established, do we need to re-create our ZNode?
                //       Or is that handled for us?
            }
        });
    }

    @Override
    public void close() {
        LOG.debug("Closing SyncZKClient now...");
    }

    @Override
    public void createAndJoinGroup(String groupName, String memberId, Invalidatable invalidatable) throws Exception {
        try {
            // This will throw an exception if the group already exists!
            createGroup(groupName);
            LOG.debug("Successfully created new ZooKeeper group '/" + groupName + "'.");
        } catch (Exception ex) {
            LOG.error("Encountered error while creating groups for " + groupName + ":", ex);
        }

        // Pass 'true' to create a PersistentWatcher object for the parent path of the group we're joining.
        joinGroupAsPermanent(groupName, memberId, invalidatable, true);
    }

    @Override
    public void addListener(String groupName, Watcher watcher) {
        String path = getPath(groupName, null, true);
        addListenerToPath(path, watcher, false);
    }

    private void addListenerToPath(String path, Watcher watcher, boolean recursive) {
        PersistentWatcher persistentWatcher = getOrCreatePersistentWatcher(path, recursive);
        persistentWatcher.getListenable().addListener(watcher);
    }

    @Override
    public ZooKeeperInvalidation getInvalidation(String path) throws Exception {
        byte[] data = client.getData().watched().forPath(path);

        return (ZooKeeperInvalidation)InvokerUtilities.bytesToObject(data);
//        ByteArrayInputStream bis = new ByteArrayInputStream(data);
//        ObjectInput in = new ObjectInputStream(bis);
//
//        return (ZooKeeperInvalidation)in.readObject();
    }

    @Override
    public void removeInvalidation(long operationId, String groupName) throws Exception {
        String invPath = getInvPath(groupName) + "/" + operationId;
        String ackPath = getAckPath(groupName) + "/" + operationId;

        List<String> acks = this.client.getChildren().forPath(ackPath);
        List<String> invs = this.client.getChildren().forPath(invPath);

        for (String ack : acks) {
            if (LOG.isDebugEnabled())
                LOG.debug("Removing ACK '" + ack + "' now...");
            this.client.delete().forPath(ackPath + "/" + ack);
        }

        for (String inv : invs) {
            if (LOG.isDebugEnabled())
                LOG.debug("Removing INV '" + inv + "' now...");
            this.client.delete().forPath(invPath + "/" + inv);
        }

        if (LOG.isDebugEnabled()) LOG.debug("Removing invalidation from ZooKeeper cluster under path: '" + invPath + "'");
        this.client.delete().forPath(invPath);

        if (LOG.isDebugEnabled()) LOG.debug("Removing ACK root from ZooKeeper cluster under path: '" + ackPath + "'");
        this.client.delete().forPath(ackPath);
    }

    @Override
    public void putInvalidation(ZooKeeperInvalidation invalidation, String groupName, Watcher watcher)
            throws Exception {
        String invPath = getInvPath(groupName) + "/" + invalidation.getOperationId();
        String ackPath = getAckPath(groupName) + "/" + invalidation.getOperationId();

        LOG.debug("Creating ACK root in ZooKeeper cluster under path: '" + ackPath + "'");

        // First, create the entry under which the other NNs will ACK.
        // Must be persistent, as we will be creating child nodes on it.
        this.client.create().withMode(CreateMode.PERSISTENT).forPath(ackPath);

        // Add the listener before issuing the actual INV.
        if (watcher != null)
            addListenerToPath(ackPath, watcher, true);

        // Then create the invalidation so they start ACKing.
        LOG.debug("Creating invalidation in ZooKeeper cluster under path: '" + invPath + "'");
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(invPath, InvokerUtilities.serializableToBytes(invalidation));
    }

    // This is called from within a Watcher, so we're given the exact path to the INV that we're acknowledging.
    @Override @Deprecated
    public void acknowledge(String path, long localNameNodeId) throws Exception {
        path += "/" + localNameNodeId;
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    @Override
    public void acknowledge(int deploymentNumber, long operationId, long localNameNodeId) throws Exception {
        String ackPath =
                getAckPath("namenode" + deploymentNumber) + "/" + operationId + "/" + localNameNodeId;

        long s = System.nanoTime();
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(ackPath);

        if (LOG.isDebugEnabled()) {
            long t = System.nanoTime();
            LOG.debug("Wrote ACK to ZK at path '" + ackPath + "' in " + ((t - s) / 1.0e6) + " ms.");
        }
    }

    // Listen for INVs directed at a particular deployment.
    @Override
    public void addInvalidationListener(String groupName, Watcher watcher) {
        String path = getInvPath(groupName);
        PersistentWatcher persistentWatcher = getOrCreatePersistentWatcher(path, true);
        persistentWatcher.getListenable().addListener(watcher);
    }

    private void addGuestListener(String groupName, Watcher watcher) {
        String path = getPath(groupName, null, false);
        PersistentWatcher persistentWatcher = getOrCreatePersistentWatcher(path, false);
        persistentWatcher.getListenable().addListener(watcher);
    }

    @Override
    public void removeListener(String groupName, Watcher watcher) {
        String path = getPath(groupName, null, true);

        PersistentWatcher persistentWatcher = watchers.getOrDefault(path, null);
        if (persistentWatcher == null) {
            LOG.error("Tried to get non-existent watcher for path " + groupName + ".");
            LOG.error("Valid watchers: " + watchers.keySet());
            throw new IllegalArgumentException("We do not have a PersistentWatcher for the path: " + path);
        }

        persistentWatcher.getListenable().removeListener(watcher);
    }

    @Override
    public void leaveGroup(String groupName, String memberId, boolean permanent) throws Exception {
        String path = getPath(groupName, memberId, permanent);

        LOG.debug("Leaving ZK group specified by path: '" + path + "'");

        this.client.delete().forPath(path);
    }

    @Override
    public List<String> getGuestGroupMembers(String groupName) throws Exception {
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        String path = getPath(groupName, null, false);

//        LOG.debug("Getting children for group: " + path);
        List<String> children = this.client.getChildren().forPath(path);

//        if (children.isEmpty())
//            LOG.warn("There are no children in group: " + path);

        return children;
    }

    @Override
    public boolean checkForPermanentGroupMember(String groupName, String memberId) throws Exception {
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        if (memberId == null)
            throw new IllegalArgumentException("Member ID argument cannot be null.");

        String path = getPath(groupName, memberId, true);

        return (client.checkExists().forPath(path) != null);
    }

    @Override
    public boolean checkForPermanentGroupMember(int deploymentNumber, String memberId) throws Exception {
        if (memberId == null)
            throw new IllegalArgumentException("Member ID argument cannot be null.");

        String path = getPath("namenode" + deploymentNumber, memberId, true);

        return (client.checkExists().forPath(path) != null);
    }

    @Override
    public List<String> getPermanentGroupMembers(String groupName) throws IOException {
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        String path = getPath(groupName, null, true);

        try {
            return this.client.getChildren().forPath(path);
        } catch (Exception ex) {
            LOG.error("Encountered exception while getting group members from ZooKeeper.");
            throw new IOException(ex);
        }
    }

    @Override
    public List<String> getAllGroupMembers(String groupName) throws Exception {
        List<String> guestMembers = getGuestGroupMembers(groupName);
        List<String> permanentMembers = getPermanentGroupMembers(groupName);
        guestMembers.addAll(permanentMembers);

        // At this point, guestMembers contains both the guest members and the permanent members.
        return guestMembers;
    }

    /**
     * Return the full path name constructed from the given parameters.
     *
     * @param groupName Corresponds to the deployment.
     * @param memberId If this is non-null, then we return the path with our memberId included at the end.
     * @param permanent If true, generate a path for the permanent sub-group. If false, generate a path
     *                  for the guest sub-group.
     */
    private String getPath(String groupName, String memberId, boolean permanent) {
        return "/" + groupName + (permanent ? PERMANENT_DIR : GUEST_DIR) + (memberId != null ? "/" + memberId : "");
    }

    /**
     * Return the path to the ACK ZNode directory for the given deployment.
     */
    private String getAckPath(String groupName) {
        return "/" + groupName + "/ACK";
    }

    /**
     * Return the path to the INV ZNode directory for the given deployment.
     */
    private String getInvPath(String groupName) {
        return "/" + groupName + "/INV";
    }
}
