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
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Encapsulates ZooKeeper/Apache Curator Framework functionality for the NameNode.
 */
public class SyncZKClient implements ZKClient {
    public static final Log LOG = LogFactory.getLog(SyncZKClient.class);

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

        this.memberId = memberId;
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
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    public Map<String, byte[]> getGroupMembers() {
        if (this.groupMember == null)
            throw new IllegalStateException("Must first join a group before retrieving group members.");

        return this.groupMember.getCurrentMembers();
    }

    @Override
    public void connect() {
        LOG.debug("Connecting to the ZK ensemble now...");
        this.client = connectToZooKeeper();
        LOG.debug("Connected successfully to ZK ensemble. Starting ZK client now...");
        this.client.start();
        LOG.debug("Successfully started ZK client.");

        AsyncCuratorFramework asyncClient = AsyncCuratorFramework.wrap(client);
    }

    @Override
    public void createGroup(String groupName) throws Exception {
        String path = "/" + groupName; // The paths must be fully-qualified, so we prepend an '/'.

        LOG.debug("Creating ZK group with path: " + path);
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
    }

    @Override
    public void createAndJoinGroup(String groupName) {
        String path = "/" + groupName; // The paths must be fully-qualified, so we prepend an '/'.

        LOG.debug("Joining ZK group via GroupMember API with path: " + path);
        this.groupMember = new GroupMember(this.client, path, this.memberId, new byte[0]);
        this.groupMember.start();
    }

    @Override
    public void joinGroup(String groupName) throws Exception {
        String path = "/" + groupName; // The paths must be fully-qualified, so we prepend an '/'.

        LOG.debug("Joining ZK group with path: " + path);
        this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    @Override
    public void close() {
        LOG.debug("Closing SyncZKClient now...");

        if (this.groupMember != null)
            this.groupMember.close();
    }

    @Override
    public GroupMember getGroupMember() {
        return this.groupMember;
    }

    @Override
    public <T> void createWatch(String groupName, Callable<T> callback) {
        if (this.client == null)
            throw new IllegalStateException("Client must be created/instantiated before any watches can be created.");
        if (groupName == null)
            throw new IllegalArgumentException("Group name argument cannot be null.");

        String path = "/" + groupName;

        LOG.debug("Synchronously creating watch for path: " + path);
    }
}
