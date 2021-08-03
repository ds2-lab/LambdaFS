package org.apache.hadoop.security.authorize;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Collection;

/**
 * A dual-entry ACL contains an ACL for a given file or directory as well as
 * the path permissions for that file or directory.
 *
 * The path permissions are the intersection of the file permissions and the parent directory's path permissions.
 *
 * See "Efficient Metadata Management in Large Distributed Storage Systems" by Brandt et al. for
 * a technical description.
 */
public class DualEntryAccessControlList extends AccessControlList {
    // register a ctor
    static {
        WritableFactories.setFactory
                (DualEntryAccessControlList.class,
                        new WritableFactory() {
                            @Override
                            public Writable newInstance() { return new DualEntryAccessControlList(); }
                        });
    }

    /**
     * This constructor exists primarily for DualEntryAccessControlList to be Writable.
     */
    public DualEntryAccessControlList() { }

    public DualEntryAccessControlList(String users, String groups) {
        super(users, groups);
    }

    public DualEntryAccessControlList(String aclString) {
        super(aclString);
    }

    /**
     * Set of users which are granted access to this path.
     */
    private Collection<String> pathUsers;

    /**
     * Set of groups which are granted access to this path.
     */
    private Collection<String> pathGroups;

    /**
     * Checks against both the single file/directory ACL and the path ACL.
     */
    @Override
    public boolean isUserAllowed(UserGroupInformation ugi) {
        if (allAllowed || users.contains(ugi.getShortUserName()))
            return true;
        else if (users.contains(ugi.getShortUserName()) && pathUsers.contains(ugi.getShortUserName()))
            return true;
        else if (!groups.isEmpty()) {
            for (String group : ugi.getGroups()) {
                if (groups.contains(group) && pathGroups.contains(group)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get the names of users allowed for this service. This is from the path ACL (rather than the file ACL).
     * @return the set of user names. the set must not be modified.
     */
    public Collection<String> getPathUsers() {
        return pathUsers;
    }

    /**
     * Get the names of user groups allowed for this service. This is from the path ACL (rather than the file ACL).
     * @return the set of group names. the set must not be modified.
     */
    public Collection<String> getPathGroups() {
        return pathGroups;
    }
}
