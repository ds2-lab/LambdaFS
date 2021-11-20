package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZKMonitor implements Runnable {
    private ZooKeeper zooKeeper;
    private Semaphore semaphore = new Semaphore(1);
    private String groupName;

    private Set<String> groupMembers;

    /**
     * Control printing between the threads.
     */
    private Lock printLock = new ReentrantLock();

    public ZKMonitor(ZooKeeper zooKeeper, String groupName) {
        this.zooKeeper = zooKeeper;
        this.groupName = groupName;
        this.groupMembers = new HashSet<>();
    }

    public static void main(String[] args) throws Exception {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("localhost", 100, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        Thread t1 = new Thread(new ZKMonitor(zk, "namenode0/permanent"));
        Thread t2 = new Thread(new ZKMonitor(zk, "namenode1/permanent"));
        Thread t3 = new Thread(new ZKMonitor(zk, "namenode2/permanent"));

        Thread t1g = new Thread(new ZKMonitor(zk, "namenode0/guest"));
        Thread t2g = new Thread(new ZKMonitor(zk, "namenode1/guest"));
        Thread t3g = new Thread(new ZKMonitor(zk, "namenode2/guest"));

        t1.start();
        t2.start();
        t3.start();

        t1g.start();
        t2g.start();
        t3g.start();
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
            System.out.println("========================================");
            System.out.println("=-------------- UPDATES --------------=");
            for (String update : updates) {
                System.out.println(update);
            }
            System.out.println("=---------- CURRENT MEMBERS ----------=");
            System.out.println(path + ": " + StringUtils.join(children, ", "));
            System.out.println("========================================");
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
