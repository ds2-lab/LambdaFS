package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Encapsulates ZooKeeper/Apache Curator Framework functionality for the NameNode.
 *
 * This is implemented using the Async API, whereas {@link SyncZKClient} is
 * purely synchronous.
 */
public class AsyncZKClient implements ZKClient {
    public static final Log LOG = LogFactory.getLog(AsyncZKClient.class);

    /**
     * Encapsulates a connection to the ZooKeeper ensemble.
     */
    private AsyncCuratorFramework client;

    /**
     * The hostnames to try connecting to.
     */
    private final String[] hosts;

    /**
     * The connection string used to connect to the ZooKeeper ensemble. Constructed from the
     * {@link AsyncZKClient#hosts} instance variable.
     */
    private final String connectionString;

    /**
     * Unique ID identifying this member in its ZK group.
     */
    private final String memberId;

    /**
     * GroupMember instance for this client.
     *
     * You can get a current view of the members by calling:
     *      groupMember.getCurrentMembers();
     */
    private GroupMember groupMember;

    /**
     * Constructor.
     * @param hosts Hostnames of the ZooKeeper servers.
     * @param memberId Unique ID identifying this member in its ZK group.
     */
    public AsyncZKClient(String[] hosts, String memberId) {
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

        this.memberId = memberId;
    }

    @Override
    public void connect() {
        LOG.debug("Connecting to the ZK ensemble now...");
        CuratorFramework syncClient = connectToZooKeeper();
        LOG.debug("Connected successfully to ZK ensemble. Starting ZK client now...");
        syncClient.start();
        LOG.debug("Successfully started ZK client.");

        this.client = AsyncCuratorFramework.wrap(syncClient);
    }

    @Override
    public void createGroup(String groupName) throws Exception {
        String path = "/" + groupName; // The paths must be fully-qualified, so we prepend an '/'.

        LOG.debug("Creating ZK group with path: " + path);
        this.client.create().withMode(CreateMode.PERSISTENT).forPath(path);
    }

    @Override
    public void joinGroup(String groupName) throws Exception {
        String path = "/" + groupName; // The paths must be fully-qualified, so we prepend an '/'.

        LOG.debug("Joining ZK group with path: " + path);
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    @Override
    public void createAndJoinGroup(String groupName) {
        throw new NotImplementedException("AsyncZKClient does not support the createAndJoinGroup() function.");
    }

    @Override
    public void close() {
        LOG.debug("Closing AsyncZKClient now...");
    }

    @Override
    public GroupMember getGroupMember() {
        return null;
    }

    @Override
    public Map<String, byte[]> getGroupMembers() {
        return null;
    }

    /**
     * Connect to the ZooKeeper ensemble.
     *
     * @return A {@link CuratorFramework object} representing the connection to the server/ensemble.
     */
    private CuratorFramework connectToZooKeeper() {
        // These are reasonable arguments for the ExponentialBackoffRetry. The first
        // retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    @Override
    public <T> void createWatch(String groupName, Callable<T> callback) {
        if (this.client == null)
            throw new IllegalStateException("Client must be created/instantiated before any watches can be created.");
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        String path = "/" + groupName;

        LOG.debug("Asynchronously creating watch for path: " + path);

        this.client.watched()
                .getData()
                .forPath(path)
                .event()
                .thenAcceptAsync(watchedEvent -> {
                    try {
                        LOG.debug("Watch on group " + path + " has been triggered!");

                        if (callback != null)
                            callback.call();
                        else
                            LOG.warn("No callback provided to do something on watch notification...");
                    } catch (Exception ex) {
                        LOG.error("Error encountered while processing event from watch on group " + path + ":", ex);
                    }
                });
    }
}

