package com.ahmet;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class WatcherDemo implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String TARGET_ZNODE = "/target_znode";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static Logger logger;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        logger = LoggerFactory.getLogger(WatcherDemo.class);

        WatcherDemo election = new WatcherDemo();
        election.connectToZookeeper();
        election.run();
        election.close();
        logger.warn("Disconnected from Zookeeper server, existing application");
    }

    private void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    private void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) {
            logger.warn(TARGET_ZNODE + " doesn't exist");
            return;
        }

        logger.warn("Node ID: " + stat.getCzxid() + ", version" + stat.getVersion());

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        logger.warn("Data: " + new String(data) + ", children: " + children);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None -> {
                if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                    logger.warn("Connected to Zookeeper Server");
                } else {
                    synchronized (zooKeeper) {
                        logger.warn("Event: Disconnected from Zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
            }
            case NodeCreated -> {
                logger.warn(TARGET_ZNODE + " was created");
            }
            case NodeDeleted -> {
                logger.warn(TARGET_ZNODE + " was deleted");
            }
            case NodeDataChanged -> {
                logger.warn(TARGET_ZNODE + " data changed");

            }
            case NodeChildrenChanged -> {
                logger.warn(TARGET_ZNODE + " children changed");
            }
//            case DataWatchRemoved -> {
//            }
//            case ChildWatchRemoved -> {
//            }
//            case PersistentWatchRemoved -> {
//            }
        }

        // Re-register after watches triggered:
        try {
            watchTargetZnode();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
