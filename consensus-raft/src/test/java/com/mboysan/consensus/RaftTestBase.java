package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import com.mboysan.consensus.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class RaftTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftTestBase.class);

    private static final long SEED = 1L;

    static {
        LOGGER.info("RaftTestBase.SEED={}", SEED);
        System.out.println("RaftTestBase.SEED=" + SEED);
    }

    private static final TimersForTesting TIMER = new TimersForTesting();
    private static Random RNG = new Random(SEED);

    RaftNode[] nodes;
    private InVMTransport transport;
    private long advanceTimeInterval = -1;
    boolean skipTeardown = false;

    @BeforeEach
    void setUp() {
        skipTeardown = false;
    }

    void init(int numServers) throws Exception {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new RaftNode[numServers];
        transport = new InVMTransport();
        for (int i = 0; i < numServers; i++) {
            RaftNode node;
            node = new RaftNodeForTesting(i, transport);
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
    void advanceTimeForElections() throws InterruptedException {
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

    int findLeaderOfMajority() {
        return Arrays.stream(nodes).sorted(Comparator.comparingInt(n -> n.getState().leaderId))
                .collect(Collectors.toList())
                .get(nodes.length / 2).getState().leaderId;
    }

    int assertLeaderChanged(int oldLeader, boolean isChangeVisibleOnOldLeader) {
        int newLeaderId;
        if (isChangeVisibleOnOldLeader) {
            newLeaderId = assertOneLeader();
        } else {
            newLeaderId = -1;
            for (RaftNode node : nodes) {
                if (node.getNodeId() == oldLeader) {
                    // the old leader should still think its the leader
                    assertEquals(oldLeader, node.getState().leaderId);
                } else {
                    if (newLeaderId == -1) {
                        newLeaderId = node.getState().leaderId;
                    }
                    assertEquals(newLeaderId, node.getState().leaderId);
                }
            }
            assertNotEquals(-1, newLeaderId);
        }
        assertNotEquals(oldLeader, newLeaderId);
        return newLeaderId;
    }

    void assertLeaderNotChanged(int currentLeaderId) {
        assertEquals(currentLeaderId, assertOneLeader());
    }

    int assertOneLeader() {
        int leaderId = -1;
        for (RaftNode node : nodes) {
            if (leaderId == -1) {
                leaderId = node.getState().leaderId;
            }
            assertEquals(leaderId, node.getState().leaderId);
        }
        assertNotEquals(-1, leaderId);
        return leaderId;
    }

    void assertLogsEquals(List<String> commands) {
        RaftLog log0 = nodes[0].getState().raftLog;
        assertEquals(commands.size(), log0.size());
        for (int i = 0; i < nodes[0].getState().raftLog.size(); i++) {
            assertEquals(commands.get(i), log0.get(i).getCommand());
        }
        for (RaftNode server : nodes) {
            assertEquals(log0, server.getState().raftLog);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(RaftNode::shutdown);
        transport.shutdown();
        TIMER.shutdown();
        RNG = new Random(SEED);
    }

    static Random getRNG() {
        return RNG;
    }

    private static class RaftNodeForTesting extends RaftNode {
        public RaftNodeForTesting(int nodeId, Transport transport) {
            super(nodeId, transport);
        }

        @Override
        Timers createTimers() {
            return TIMER;
        }
    }
}
