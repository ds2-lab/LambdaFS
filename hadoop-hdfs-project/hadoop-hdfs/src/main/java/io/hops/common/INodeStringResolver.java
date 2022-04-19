package io.hops.common;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;

import java.util.NoSuchElementException;

/**
 * Identical implementation and function to {@link INodeResolver}, except the type of this class'
 * {@code components} field is String[] rather than byte[][].
 *
 * The purpose is to avoid creating all the byte[] objects that get created when using {@link INodeResolver}.
 */
public class INodeStringResolver {
    private final String[] components;
    private final boolean resolveLink;
    private final boolean transactional;
    private INode currentInode;
    private int count = 0;
    private int depth = INodeDirectory.ROOT_DIR_DEPTH;
    private boolean canCheckCache;

    public INodeStringResolver(String[] components, INode baseINode,
                         boolean resolveLink, boolean transactional) {
        this(components, baseINode, resolveLink, transactional, true);
    }

    public INodeStringResolver(String[] components, INode baseINode, boolean resolveLink,
                         boolean transactional, boolean canCheckCache) {
        this.components = components;
        this.currentInode = baseINode;
        this.resolveLink = resolveLink;
        this.transactional = transactional;
        this.canCheckCache = canCheckCache;
    }

    public INodeStringResolver(String[] components, INode baseINode,
                         boolean resolveLink, boolean transactional, int initialCount) {
        this(components, baseINode, resolveLink, transactional, initialCount, true);
    }

    public INodeStringResolver(String[] components, INode baseINode,
                         boolean resolveLink, boolean transactional, int initialCount, boolean canCheckCache) {
        this(components, baseINode, resolveLink, transactional, canCheckCache);
        this.count = initialCount;
        this.depth = INodeDirectory.ROOT_DIR_DEPTH + (initialCount);
    }

    public boolean hasNext() {
        if (currentInode == null) {
            return false;
        }
        if (currentInode.isFile()) {
            return false;
        }
        return count + 1 < components.length;
    }

    public INode next() throws UnresolvedPathException, StorageException,
            TransactionContextException {
        boolean lastComp = (count == components.length - 1);
        if (currentInode.isSymlink() && (!lastComp || resolveLink)) {
            final String symPath =
                    INodeUtil.constructPath(components, 0, components.length);
            final String preceding = INodeUtil.constructPath(components, 0, count);
            final String remainder =
                    INodeUtil.constructPath(components, count + 1, components.length);
            final String link = components[count];
            final String target = ((INodeSymlink) currentInode).getSymlinkString();
            if (ServerlessNameNode.stateChangeLog.isDebugEnabled()) {
                ServerlessNameNode.stateChangeLog.debug(
                        "UnresolvedPathException " + " path: " + symPath + " preceding: " +
                                preceding + " count: " + count + " link: " + link +
                                " target: " + target + " remainder: " + remainder);
            }
            throw new UnresolvedPathException(symPath, preceding, remainder, target);
        }

        if (!hasNext()) {
            throw new NoSuchElementException(
                    "Trying to read more components than available");
        }

        depth++;
        count++;
        long partitionId = INode.calculatePartitionId(currentInode.getId(), components[count], (short) depth);

        currentInode = INodeUtil
                .getNode(components[count], currentInode.getId(), partitionId, transactional, canCheckCache);
        return currentInode;
    }

    public int getCount() {
        return count;
    }
}