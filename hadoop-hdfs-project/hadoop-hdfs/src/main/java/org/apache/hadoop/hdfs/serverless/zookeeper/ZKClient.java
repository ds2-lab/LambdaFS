package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * The API of a ZooKeeper client.
 *
 * Concrete implementations of this interface come in one of two flavors: synchronous and asynchronous.
 *
 * The synchronous version is implemented by {@link SyncZKClient}. There is presently no asynchronous version.
 */
public interface ZKClient {
    /**
     * Connect to the ZooKeeper ensemble.
     */
    void connect();

    /**
     * Create a ZooKeeper group/directory (i.e., a persistent ZNode) with the given name.
     *
     * One can think of ZooKeeper as a bunch of directories (so, think of a file tree). Clients create these
     * things called ZNodes, which can be thought of as directories with extra functionality. There are a few
     * different kinds of ZNodes we can create.
     *
     * Persistent ZNodes do not get deleted after the client who created them disconnects. So, if Client1 creates
     * a persistent ZNode "/foo" and then disconnects, the "/foo" ZNode will still exist in ZooKeeper. This is useful
     * for creating the group itself.
     *
     * Clients can then join this group by creating a so-called "ephemeral" ZNode. Ephemeral ZNodes are deleted after
     * the client who created them disconnects.
     *
     * So let's say we want to have a group called "TheGroup". We can create a persistent ZNode "/TheGroup". Then,
     * clients can join the group by creating ephemeral, child ZNodes of "/TheGroup". For example, Client1 could create
     * an ephemeral ZNode "/TheGroup/Client1". As long as the "/TheGroup/Client1" ZNode exists, everybody in the group
     * knows that Client1 is active. Maybe Client2 will add its own ephemeral ZNode "/TheGroup/Client2", so now there
     * are two ephemeral ZNodes ("/TheGroup/Client1" and "/TheGroup/Client2") and one persistent ZNode ("/TheGroup").
     *
     * So, this method creates a PERSISTENT ZNode.
     *
     * NOTE: This also creates the persistent ZNodes for the permanent and guest sub-groups.
     *
     * @param groupName The name to use for the new group/directory.
     *
     * @throws KeeperException.NodeExistsException If the group already exists (i.e., if there is already a persistent
     * node with the same path).
     */
    void createGroup(String groupName) throws Exception, KeeperException.NodeExistsException;

    /**
     * Check if the NameNode identified by the given ID exists in the specified group.
     * @param groupName The group in which we're checking for membership.
     * @param memberId The NameNode in question.
     *
     * @return True if the NN exists/is a member, otherwise false.
     */
    boolean checkForPermanentGroupMember(String groupName, String memberId) throws Exception;

    /**
     * Check if the NameNode identified by the given ID exists in the specified deployment.
     *
     * This assumes the group name for the deployment is "namenode[deploymentNumber]".
     *
     * @param deploymentNumber The deployment in which we're checking for membership.
     * @param memberId The NameNode in question.
     *
     * @return True if the NN exists/is a member, otherwise false.
     */
    boolean checkForPermanentGroupMember(int deploymentNumber, String memberId) throws Exception;

    /**
     * Join an existing ZooKeeper group/directory by creating an ephemeral child node under
     * the parent directory. We specifically join the PERMANENT sub-group.
     *
     * The ephemeral ZNode will exist as long as we (the client who created it) are alive and connected to ZooKeeper.
     * If we disconnect, then our ZNode will be deleted, signifying that we have left the group and are no longer
     * running.
     *
     * @param groupName The ZK directory/group to join.
     * @param memberId Used when creating the ephemeral ID of this node.
     * @param invalidatable Entity with a cache that can be invalidated. We use the {@link Invalidatable} interface
     *                      to hook into the object and invalidate its cache when connection to ZK is lost.
     * @param createWatch If true, create a PersistentWatcher instance to monitor changes in group membership.
     */
    void joinGroupAsPermanent(String groupName, String memberId, Invalidatable invalidatable, boolean createWatch)
            throws Exception;

    /**
     * Join an existing ZooKeeper group. We specifically join the GUEST sub-group.
     * @param groupName The ZK directory/group to join.
     * @param memberId Used when creating the ephemeral ID of this node.
     * @param watcher Used as a callback for membership changes. We register these at the beginning, right when
     *                we join the group, as that's how guest-joins are used.
     * @param watcherOption Specifies the desired option for creating a PersistentWatcher.
     */
    void joinGroupAsGuest(String groupName, String memberId, Watcher watcher,
                          GuestWatcherOption watcherOption) throws Exception;

    /**
     * Leave the group by deleting the ephemeral ZNode indicated by the path /groupName/permanent/memberId if
     * permanent is true, or /groupName/guest/memberId if permanent is false.
     * @param groupName Corresponds to the deployment.
     * @param memberId The ID of the NameNode that is leaving.
     * @param permanent If true, leave the permanent sub-group. If false, leave the guest sub group.
     * @throws Exception
     */
    void leaveGroup(String groupName, String memberId, boolean permanent) throws Exception;

    /**
     * Create and join a group with the given name. This just calls the `createGroup()` function followed by the
     * `joinGroup()` function.
     *
     * IMPORTANT: This function wraps the call to `createGroup()` in a try-catch and ignores the possible
     * NodeExistsException that gets thrown if the group already exists. During standard operation, the groups will
     * almost always already exist, so the `createGroup()` call is expected to throw an exception.
     *
     * NOTE: This will always join the PERMANENT sub-group for a particular deployment.
     * NOTE: This will create a PersistentWatcher object for the parent path of the group we're joining.
     *
     * @param groupName The name of the group to join.
     * @param memberId Used when creating the ephemeral ID of this node.
     * @param invalidatable Entity with a cache that can be invalidated. We use the {@link Invalidatable} interface
     *                      to hook into the object and invalidate its cache when connection to ZK is lost.
     */
    void createAndJoinGroup(String groupName, String memberId, Invalidatable invalidatable) throws Exception;

    /**
     * Perform any necessary clean-up, such as stopping a
     * {@link org.apache.curator.framework.recipes.nodes.GroupMember} instance.
     */
    void close();

    /**
     * Add a listener to the Watch for the given group. The listener will call the provided callback.
     * The callback should handle determining whether to act depending on which event fired.
     *
     * This adds a listener to the PERMANENT sub-group.
     *
     * This will create and start a Persistent Watcher for the generated path if one does not already exist.
     *
     * @param groupName The name of the group for which a listener will be added. If we are not in this group, then
     *                  this will probably fail.
     * @param watcher Watcher object to be added. Serves as the callback for the event notification.
     */
    void addListener(String groupName, Watcher watcher);

    /**
     * Retrieve the invalidation (specifically its data) at the given path.
     * @param path Path of the ZNode, which is serving as an invalidation.
     */
    ZooKeeperInvalidation getInvalidation(String path) throws Exception;

    /**
     * Acknowledge the invalidation at the given path.
     * @param path The path of the INV that we're acknowledging.
     * @param localNameNodeId The unique ID of the local NameNode instance.
     */
    @Deprecated
    void acknowledge(String path, long localNameNodeId) throws Exception;

    /**
     * Acknowledge the invalidation we received.
     *
     * @param deploymentNumber The deployment that was targeted by the INV.
     * @param operationId The unique ID of the write operation.
     * @param localNameNodeId The ID of the NameNode performing the ACK.
     */
    void acknowledge(int deploymentNumber, long operationId, long localNameNodeId) throws Exception;

    /**
     * Add a listener to the Watch for the INV ZNode of the specified group.
     * The listener will call the provided callback.
     * The callback should handle determining whether to act depending on which event fired.
     *
     * This will create and start a Persistent Watcher for the generated path if one does not already exist.
     * Note that the Persistent Watcher created by this function WILL be recursive, unlike the one created by
     * the {@code addListener} function.
     *
     * @param groupName The name of the group for which a listener will be added to the INV ZNode.
     *                  If we are not in this group, then this will probably fail.
     * @param watcher Watcher object to be added. Serves as the callback for the event notification.
     */
    void addInvalidationListener(String groupName, Watcher watcher);

    /**
     * Add an invalidation to the INV ZNode directory for the specified group. The group name should be
     * "namenode[deployment_number]".
     *
     * @param invalidation Encapsulates all the information needed by follower Lambdas.
     * @param groupName The deployment for which we're issuing the invalidation.
     * @param watcher Used to set up a watch on the newly-created ZNode. It is technically possible for an event to
     *                happen between creating the ZNode and this watch being established, so it is necessary to check
     *                for any state changes explicitly despite creating this watch.
     *
     *                The watch that gets created will be recursive, with the intent of listening for
     *                {@code NodeCreated} events.
     */
    void putInvalidation(ZooKeeperInvalidation invalidation, String groupName, Watcher watcher) throws Exception;

    /**
     * Remove the ZNode associated with the given invalidation.
     *
     * This will first try to remove the child nodes of the specified invalidation.
     *
     * This also removes the associated ACK root (and any ACKs) by the same process (i.e., delete children
     * of ACK root first, then delete ACK root itself).
     *
     * @param operationId The unique ID of the operation associated with the invalidation to be removed.
     * @param groupName Essentially the deployment for which the INV was issued and for which we'll delete the INV.
     */
    void removeInvalidation(long operationId, String groupName) throws Exception;

    /**
     * Remove a listener from the Watch for the given group. This removes the watcher from the PERMANENT sub-group.
     *
     * @param groupName The name of the group for which the listener will be removed. If we are not in this group,
     *                  then this will probably fail.
     * @param watcher Watcher object to be removed.
     */
    void removeListener(String groupName, Watcher watcher);

    /**
     * Get the children of the given group. This returns PERMANENT group members.
     *
     * @param groupName The group whose children we desire.
     * @return List of IDs of the members of the permanent sub-group of the specified deployment group.
     */
    public List<String> getPermanentGroupMembers(String groupName) throws IOException;

    /**
     * Get the children of the given group. This returns GUEST group members.
     *
     * @param groupName The group whose children we desire.
     * @return List of IDs of the members of the guest sub-group of the specified deployment group.
     */
    public List<String> getGuestGroupMembers(String groupName) throws Exception;

    /**
     * Get the group members for both the permanent and guest sub-groups of a particular deployment.
     *
     * @param groupName Identifies the deployment group in question.
     * @return List of IDs of members from the guest and permanent sub-groups of the specified deployment group.
     */
    public List<String> getAllGroupMembers(String groupName) throws Exception;

//    /**
//     * Return the GroupMember instance associated with this client, if it exists. Otherwise, return null.
//     */
//    GroupMember getGroupMember();

//    /**
//     * Get the members of the group we're currently in.
//     */
//    Map<String, byte[]> getGroupMembers();

//    /**
//     * Create a watch on the given group. Can optionally supply a callback to process the event.
//     *
//     * @param groupName The name of the group for which to create a watch.
//     */
//    <T> void createWatch(String groupName, Callable<T> callback);
}
