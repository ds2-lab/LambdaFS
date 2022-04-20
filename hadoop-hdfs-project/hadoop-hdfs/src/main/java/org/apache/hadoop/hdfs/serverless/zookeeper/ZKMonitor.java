package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Time;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZKMonitor implements Runnable {
    public static final Log LOG = LogFactory.getLog(ZKMonitor.class);
    private ZooKeeper zooKeeper;
    private Semaphore semaphore = new Semaphore(1);
    private String groupName;

    private Set<String> groupMembers;

    private final int deploymentNumber;

    /**
     * Indicates whether this instance of ZKMonitor is monitoring permanent or guest group membership.
     */
    private final boolean permanentMonitor;

    /**
     * Control printing between the threads.
     */
    private static Lock printLock = new ReentrantLock();

    public ZKMonitor(ZooKeeper zooKeeper, String groupName, int deploymentNumber, boolean permanentMonitor) {
        this.zooKeeper = zooKeeper;
        this.groupName = groupName;
        this.groupMembers = new HashSet<>();
        this.deploymentNumber = deploymentNumber;
        this.permanentMonitor = permanentMonitor;
    }

    public static void main(String[] args) throws Exception {
        String log4jconfigPath = "/home/ubuntu/slf4j/zkmonitor-cfg/zkmonitor-log4j.properties";
        PropertyConfigurator.configure(log4jconfigPath);

        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("localhost", 100, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        Thread t1 = new Thread(new ZKMonitor(zk, "namenode0/permanent",
                0, true));
        Thread t2 = new Thread(new ZKMonitor(zk, "namenode1/permanent",
                1, true));
        Thread t3 = new Thread(new ZKMonitor(zk, "namenode2/permanent",
                2, true));

//        Thread t1g = new Thread(new ZKMonitor(zk, "namenode0/guest",
//                0, false));
//        Thread t2g = new Thread(new ZKMonitor(zk, "namenode1/guest",
//                1, false));
//        Thread t3g = new Thread(new ZKMonitor(zk, "namenode2/guest",
//                2, false));

        t1.start();
        t2.start();
        t3.start();

//        t1g.start();
//        t2g.start();
//        t3g.start();
    }

    public void listForever()
            throws KeeperException, InterruptedException {
        semaphore.acquire();
        while (true) {
            list(groupName);
            semaphore.acquire();
        }
    }

    private void list(String groupName)
            throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        List<String> children = zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    semaphore.release();
                }
            }
        });

        // Accumulate messages to print here. Then we'll grab the print lock, print everything, then unlock.
        List<String> updates = new ArrayList<>();

        if (children.size() > 0) {
            if (groupMembers.size() == 0) {
                String msg = children.size() + " new NNs joined " + groupName + ": " +
                        StringUtils.join(children, ", ");
                updates.add(msg);
                groupMembers.addAll(children);
            }
            else {
                for (String id : children) {
                    List<String> newMembers = new ArrayList<>();
                    if (!(groupMembers.contains(id))) {
                        newMembers.add(id);
                    }
                    if (newMembers.size() > 0) {
                        String msg = newMembers.size() + " new NNs joined " + groupName + ": " +
                                StringUtils.join(newMembers, ", ");
                        updates.add(msg);
                        groupMembers.addAll(newMembers);
                    }
                }
            }
        }

        List<String> removed = new ArrayList<>();
        for (String currentMemberId : groupMembers) {
            if (!(children.contains(currentMemberId))) {
                removed.add(currentMemberId);
            }
        }

        if (removed.size() > 0) {
            String msg = removed.size() + " NNs left " + groupName + ": " +
                    StringUtils.join(removed, ", ");
            updates.add(msg);
            groupMembers.removeAll(removed);
        }

        if (children.isEmpty()) {
            String msg = String.format("No members in group %s\n", groupName);
            updates.add(msg);
            return;
        }
        Collections.sort(children);

        printLock.lock();
        try {
            LOG.debug("==================================================");

            if (permanentMonitor)
                LOG.debug("=--------- " + groupName + " P-UPDATES ---------=");
            else
                LOG.debug("=--------- " + groupName + " G-UPDATES ---------=");

            for (String update : updates) {
                LOG.debug(update);
            }

            if (permanentMonitor)
                LOG.debug("=-------- CURRENT DEPLOYMENT #" + deploymentNumber + " P-MEMBERS --------=");
            else
                LOG.debug("=-------- CURRENT DEPLOYMENT #" + deploymentNumber +  " G-MEMBERS --------=");

            LOG.debug(path + ": " + StringUtils.join(children, ", "));
            LOG.debug("==================================================");
            LOG.debug("");
        } finally {
            printLock.unlock();
        }
    }

    @Override
    public void run() {
        try {
            listForever();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
