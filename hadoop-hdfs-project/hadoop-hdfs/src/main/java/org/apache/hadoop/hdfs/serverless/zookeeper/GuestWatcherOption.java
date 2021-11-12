package org.apache.hadoop.hdfs.serverless.zookeeper;

/**
 * Defines the various options for creating a PersistentWatcher when joining another deployment as a guest.
 */
public enum GuestWatcherOption {
    /**
     * When joining a deployment as a guest, create a PersistentWatcher to monitor the permanent sub-group for
     * membership changes.
     */
    CREATE_WATCH_ON_PERMANENT,

    /**
     * When joining a deployment as a guest, create a PersistentWatcher to monitor the guest sub-group for
     * membership changes.
     */
    CREATE_WATCH_ON_GUEST,

    /**
     * When joining a deployment as a guest, create a PersistentWatcher to monitor both the permanent sub-group and
     * the guest sub-group for membership changes.
     */
    CREATE_WATCH_ON_BOTH,

    /**
     * When joining a deployment as a guest, do not create a PersistentWatcher for either sub-group.
     */
    DO_NOT_CREATE
}
