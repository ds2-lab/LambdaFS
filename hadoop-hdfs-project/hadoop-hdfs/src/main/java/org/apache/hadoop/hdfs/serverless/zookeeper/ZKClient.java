package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * The public API of a ZooKeeper client.
 *
 * Concrete implementations of this interface come in one of two flavors: synchronous and asynchronous.
 *
 * The synchronous version is implemented by {@link SyncZKClient}, while the asynchronous
 * version is implemented by {@link AsyncZKClient}.
 */
public interface ZKClient {
    /**
     * Connect to the ZooKeeper ensemble.
     */
    public void connect();

    /**
     * Create a ZooKeeper group/directory (i.e., a persistent ZNode) with the given name.
     * @param groupName The name to use for the new group/directory.
     */
    public void createGroup(String groupName) throws Exception;

    /**
     * Join an existing ZooKeeper group/directory by creating an ephemeral child node under
     * the parent directory.
     * @param groupName The ZK directory/group to join.
     * @param memberId Used when creating the ephemeral ID of this node.
     */
    public void joinGroup(String groupName, String memberId) throws Exception;

    /**
     * Create and join a group with the given name.
     * @param groupName The name of the group to join.
     * @param memberId Used when creating the ephemeral ID of this node.
     */
    public void createAndJoinGroup(String groupName, String memberId) throws Exception;

    /**
     * Perform any necessary clean-up, such as stopping a
     * {@link org.apache.curator.framework.recipes.nodes.GroupMember} instance.
     */
    public void close();

    /**
     * Get the children of the given group. This creates a watcher for the children of that group changing
     * and calls the specified callback when the children of the group change.
     * @param groupName The group whose children we desire.
     * @param callback The function to call when the group membership changes. Note that this is NOT run in a different
     *                 thread.
     * @return List of IDs of the members of the specified group.
     */
    public List<String> getGroupMembers(String groupName, Runnable callback) throws Exception;

    /**
     * Get the children of the given group.
     * @param groupName The group whose children we desire..
     * @return List of IDs of the members of the specified group.
     */
    public List<String> getGroupMembers(String groupName) throws Exception;

//    /**
//     * Return the GroupMember instance associated with this client, if it exists. Otherwise, return null.
//     */
//    public GroupMember getGroupMember();

//    /**
//     * Get the members of the group we're currently in.
//     */
//    public Map<String, byte[]> getGroupMembers();

//    /**
//     * Create a watch on the given group. Can optionally supply a callback to process the event.
//     *
//     * @param groupName The name of the group for which to create a watch.
//     */
//    public <T> void createWatch(String groupName, Callable<T> callback);
}
