package org.apache.hadoop.hdfs.serverless.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class ZKMonitor implements Runnable {
    private ZooKeeper zooKeeper;
    private Semaphore semaphore = new Semaphore(1);
    private String groupName;

    public ZKMonitor(ZooKeeper zooKeeper, String groupName) {
        this.zooKeeper = zooKeeper;
        this.groupName = groupName;
    }

    public static void main(String[] args) throws Exception {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("localhost", 100, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        Thread t1 = new Thread(new ZKMonitor(zk, "/namenode0/permanent"));
        Thread t2 = new Thread(new ZKMonitor(zk, "/namenode1/permanent"));
        Thread t3 = new Thread(new ZKMonitor(zk, "/namenode2/permanent"));

        Thread t1g = new Thread(new ZKMonitor(zk, "/namenode0/guest"));
        Thread t2g = new Thread(new ZKMonitor(zk, "/namenode1/guest"));
        Thread t3g = new Thread(new ZKMonitor(zk, "/namenode2/guest"));

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
        if (children.isEmpty()) {
            System.out.printf("No members in group %s\n", groupName);
            return;
        }
        Collections.sort(children);
        System.out.println(path + ": " + children);
        System.out.println("--------------------");
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
