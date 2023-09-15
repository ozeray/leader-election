package com.ahmet;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderReelection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ELECTION_NAMESPACE = "/election";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private String currentZnodeName;
    private static Logger logger;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        logger = LoggerFactory.getLogger(LeaderReelection.class);

        LeaderReelection election = new LeaderReelection();
        election.connectToZookeeper();
        election.volunteerForLeadership();
        election.reelectLeader();
        election.run();
        election.close();
        logger.warn("Disconnected from Zookeeper server, existing application");
    }

    private void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.warn("Znode name: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        logger.warn("My znode name: " + currentZnodeName);
    }

    private void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                logger.warn("I am the leader");
                return;
            } else {
                logger.warn("I'm not the leader, " + smallestChild + " is the leader");

                int currentZnodeIndex = Collections.binarySearch(children, currentZnodeName);

                // I will watch the znode that is just before me in the hierarchy:
                int predecessorIndex = currentZnodeIndex - 1;

                // Find the name of the watched znode:
                predecessorZnodeName = children.get(predecessorIndex);
                // znode indexed with predecessorIndex could be deleted just here. So, we use while (predecessorStat == null).

                // Watch the predecessor znode:
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        logger.warn("Watching znode: " + predecessorZnodeName);
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
            case NodeDeleted -> {
                try {
                    reelectLeader();
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
