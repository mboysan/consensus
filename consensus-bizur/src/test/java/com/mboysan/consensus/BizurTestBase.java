package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import com.mboysan.consensus.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BizurTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BizurTestBase.class);

    private static final long SEED = 1L;
    static {
        LOGGER.info("RaftTestBase.SEED={}", SEED);
        System.out.println("RaftTestBase.SEED=" + SEED);
    }

    private static final TimersForTesting TIMER = new TimersForTesting();
    private static Random RNG = new Random(SEED);

    BizurNode[] nodes;
    private InVMTransport transport;
    private long advanceTimeInterval = -1;
    boolean skipTeardown = false;

    @BeforeEach
    void setUp() {
        skipTeardown = false;
    }

    void init(int numServers) throws Exception {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new BizurNode[numServers];
        transport = new InVMTransport();
        for (int i = 0; i < numServers; i++) {
            BizurNode node;
            node = new BizurNodeForTesting(i, transport);
            nodes[i] = node;
            futures.add(node.start());

            advanceTimeInterval = Math.max(advanceTimeInterval, node.electionTimeoutMs);
        }

        advanceTimeForElections();
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    /**
     * Advances time to try triggering election on all nodes.
     */
    void advanceTimeForElections() {
        // use fake timer
        // following loop should allow triggering election on the node with slowest electionTimer
        TIMER.advance(advanceTimeInterval * 2);
    }

    void disconnect(int nodeId) {
        transport.connectedToNetwork(nodeId, false);
    }

    void connect(int nodeId) {
        transport.connectedToNetwork(nodeId, true);
    }

    void kill(int nodeId) {
        TIMER.pause("updateTimer-node" + nodeId);
        disconnect(nodeId);
    }

    void revive(int nodeId) {
        TIMER.resume("updateTimer-node" + nodeId);
        connect(nodeId);
    }

    void assertLeaderNotChanged(int currentLeaderId) {
        assertEquals(currentLeaderId, assertOneLeader());
    }

    int assertOneLeader() {
        int leaderId = -1;
        for (BizurNode node : nodes) {
            if (leaderId == -1) {
                leaderId = node.getState().getLeaderId();
            }
            assertEquals(leaderId, node.getState().getLeaderId());
        }
        assertNotEquals(-1, leaderId);
        return leaderId;
    }

    int assertLeaderChanged(int oldLeader, boolean isChangeVisibleOnOldLeader) {
        int newLeaderId;
        if (isChangeVisibleOnOldLeader) {
            newLeaderId = assertOneLeader();
        } else {
            newLeaderId = -1;
            for (BizurNode node : nodes) {
                if (node.getNodeId() == oldLeader) {
                    // the old leader should still think its the leader
                    assertEquals(oldLeader, node.getState().getLeaderId());
                } else {
                    if (newLeaderId == -1) {
                        newLeaderId = node.getState().getLeaderId();
                    }
                    assertEquals(newLeaderId, node.getState().getLeaderId());
                }
            }
            assertNotEquals(-1, newLeaderId);
        }
        assertNotEquals(oldLeader, newLeaderId);
        return newLeaderId;
    }

    int findLeaderOfMajority() {
        return Arrays.stream(nodes).sorted(Comparator.comparingInt(n -> n.getState().getLeaderId()))
                .collect(Collectors.toList())
                .get(nodes.length/ 2).getState().getLeaderId();
    }

    void assertLeaderOfMajority(int majorityLeaderId) {
        assertEquals(majorityLeaderId, findLeaderOfMajority());
    }

    void assertBucketMapsEquals(Map<String, String> expectedBucketMap) {
        for (BizurNode node : nodes) {
            assertBucketMapsEquals(node.getNodeId(), expectedBucketMap);
        }
    }

    void assertBucketMapsEquals(int nodeId, Map<String, String> expectedBucketMap) {
        int totalSize = 0;
        for (Integer bucketIndex : nodes[nodeId].getBucketMap().keySet()) {
            Bucket bucket = nodes[nodeId].getBucketMap().get(bucketIndex);
            for (String key : bucket.getKeySetOp()) {
                assertEquals(expectedBucketMap.get(key), bucket.getOp(key));
                totalSize++;
            }
        }
        assertEquals(expectedBucketMap.size(), totalSize);
    }


    @AfterEach
    void tearDown() throws Exception {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(BizurNode::shutdown);
        transport.shutdown();
        TIMER.shutdown();
        RNG = new Random(SEED);
    }

    static Random getRNG() {
        return RNG;
    }

    private static class BizurNodeForTesting extends BizurNode {
        public BizurNodeForTesting(int nodeId, Transport transport) {
            super(nodeId, transport);
        }

        @Override
        Timers createTimers() {
            return TIMER;
        }
    }

}
