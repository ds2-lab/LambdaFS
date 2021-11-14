package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.*;

import java.util.HashMap;
import java.util.List;

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
    private final HashMap<String, PersistentWatcher> watchers;

    /**
     * Constructor.
     * @param hosts Hostnames of the ZooKeeper servers.
     * @param memberId Unique ID identifying this member in its ZK group.
     */
    public SyncZKClient(String[] hosts, String memberId) {
        if (hosts == null)
            throw new IllegalArgumentException("The 'hosts' array argument must be non-null.");

        if (hosts.length == 0)
            throw new IllegalArgumentException("The 'hosts' array argument must have length greater than zero.");

        this.hosts = hosts;

        StringBuilder connectionStringBuilder = new StringBuilder();
        for (int i = 0; i < this.hosts.length; i++) {
            String host = this.hosts[i];

            connectionStringBuilder.append(host);

            if (i < this.hosts.length - 1)
                connectionStringBuilder.append(',');
        }
        this.connectionString = connectionStringBuilder.toString();
        this.watchers = new HashMap<>();
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
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 5);

        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
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
    public void createGroup(String groupName) throws Exception, KeeperException.NodeExistsException {
        if (this.client == null)
            throw new IllegalStateException("ZooKeeper client must be instantiated before joining a group.");

        if (groupName == null)
            throw new IllegalArgumentException("Group name parameter must be non-null.");

        // Permanent refers to the fact that this is the path for permanent group members.
        String permanentPath = getPath(groupName, null, true);
        String guestPath = getPath(groupName, null, false);

        LOG.debug("Creating ZK group with path: " + permanentPath);
        // This will throw an exception if the group already exists!
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(permanentPath);

        LOG.debug("Creating ZK group with path: " + guestPath);
        // This will throw an exception if the group already exists!
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(guestPath);
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
            String watcherPath = getPath(groupName, null, true);
            createMemembershipChangedWatcher(watcherPath, false, watcher);
        }
        else if (watcherOption == GuestWatcherOption.CREATE_WATCH_ON_GUEST) {
            LOG.debug("Creating watcher on guest sub-group for group " + groupName + ".");
            String watcherPath = getPath(groupName, null, false);
            createMemembershipChangedWatcher(watcherPath, false, watcher);
        }
        else if (watcherOption == GuestWatcherOption.CREATE_WATCH_ON_BOTH) {
            LOG.debug("Creating watcher on both the permanent and guest sub-groups for group " + groupName + ".");
            String watcherPathPermanent = getPath(groupName, null, true);
            String watcherPathGuest = getPath(groupName, null, true);
            createMemembershipChangedWatcher(watcherPathPermanent, false, watcher);
            createMemembershipChangedWatcher(watcherPathGuest, false, watcher);
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
            createStateChangedWatcher(parentPath, false, invalidatable);
        } else {
            LOG.warn("Skipping creation of PersistentWatcher for parent path '" + parentPath + "'");
        }
    }

    /**
     * Create and start a PersistentWatcher for the given path.
     * @param path The path for which to create the PersistentWatcher.
     * @param recursive Passed to the PersistentWatcher constructor.
     * @return the created PersistentWatcher instance. Note that the instance will already be started (i.e., it will
     * have had .start() called on it).
     */
    private PersistentWatcher createAndStartPersistentWatcher(String path, boolean recursive) {
        PersistentWatcher persistentWatcher = new PersistentWatcher(this.client, path, recursive);
        persistentWatcher.start();

        if (this.watchers.containsKey(path))
            LOG.warn("We already have a watcher for path " + path + ".");
        else
            this.watchers.put(path, persistentWatcher);

        return persistentWatcher;
    }

    /**
     * Create a PersistentWatcher instance that monitors for membership changes on the given path and calls the
     * provided Watcher as a callback when events occur.
     * @param path Path for which the PersistentWatcher will be created and for which membership changes will
     *             be monitored.
     * @param recursive Passed to the PersistentWatcher.
     * @param watcher Callback used when membership changes occur.
     */
    private void createMemembershipChangedWatcher(String path, boolean recursive, Watcher watcher) {
        PersistentWatcher persistentWatcher = createAndStartPersistentWatcher(path, recursive);
        persistentWatcher.getListenable().addListener(watcher);
    }

    /**
     * Create and start a PersistentWatcher instance for the given path.
     *
     * @param path The path for which the PersistentWatcher is created.
     * @param recursive Passed to the PersistentWatcher constructor. For now, this will always be false.
     * @param invalidatable Used as a callback for state changes detected by the PersistentWatcher.
     *                      If null, then no callback occurs, but a message is logged.
     */
    private void createStateChangedWatcher(String path, boolean recursive, Invalidatable invalidatable) {
        PersistentWatcher persistentWatcher = createAndStartPersistentWatcher(path, recursive);

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
        } catch (KeeperException.NodeExistsException ex) {
            LOG.debug("ZooKeeper group '/" + groupName + "' already exists.");
        }

        // Pass 'true' to create a PersistentWatcher object for the parent path of the group we're joining.
        joinGroupAsPermanent(groupName, memberId, invalidatable, true);
    }

    @Override
    public void addListener(String groupName, Watcher watcher) {
        String path = getPath(groupName, null, true);

        PersistentWatcher persistentWatcher = watchers.getOrDefault(path, null);
        if (persistentWatcher == null) {
            LOG.error("Tried to get non-existent watcher for path " + groupName + ".");
            LOG.error("Valid watchers: " + watchers.keySet());
            throw new IllegalArgumentException("We do not have a PersistentWatcher for the path: " + path);
        }

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
    public List<String> getPermanentGroupMembers(String groupName) throws Exception {
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        String path = getPath(groupName, null, true);

//        LOG.debug("Getting children for group: " + path);
        List<String> children = this.client.getChildren().forPath(path);

//        if (children.isEmpty())
//            LOG.warn("There are no children in group: " + path);

        return children;
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
}
