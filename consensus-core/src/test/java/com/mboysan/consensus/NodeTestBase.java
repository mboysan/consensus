package com.mboysan.consensus;

import com.mboysan.consensus.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

abstract class NodeTestBase<N extends AbstractNode<?>> implements NodeInternals<N> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTestBase.class);

    private static final long SEED = 1L;

    private static final TimersForTesting TIMER = new TimersForTesting();
    private static Random RNG = new Random(SEED);

    private N[] nodes;
    private InVMTransport transport;
    private long advanceTimeInterval = -1;
    boolean skipTeardown = false;

    @BeforeEach
    void setUp() {
        skipTeardown = false;
    }

    @SuppressWarnings("unchecked")
    void init(int numServers) throws Exception {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = (N[]) Array.newInstance(getNodeType(), numServers);
        transport = new InVMTransport();
        for (int i = 0; i < numServers; i++) {
            N node = createNode(i, transport, TIMER);
            nodes[i] = node;
            futures.add(node.start());
        }
        advanceTimeInterval = getElectionTimeoutOf(nodes[0]) * nodes.length;
        LOGGER.info("advanceTimeInterval={}", advanceTimeInterval);

        advanceTimeForElections();
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    Transport getTransport() {
        return transport;
    }

    N getNode(int index) {
        return nodes[index];
    }
    N[] getNodes() {
        return nodes;
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
        for (N node : nodes) {
            if (leaderId == -1) {
                leaderId = getLeaderIdOf(node);
            }
            assertEquals(leaderId, getLeaderIdOf(node));
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
            for (N node : nodes) {
                if (node.getNodeId() == oldLeader) {
                    // the old leader should still think its the leader
                    assertEquals(oldLeader, getLeaderIdOf(node));
                } else {
                    if (newLeaderId == -1) {
                        newLeaderId = getLeaderIdOf(node);
                    }
                    assertEquals(newLeaderId, getLeaderIdOf(node));
                }
            }
            assertNotEquals(-1, newLeaderId);
        }
        assertNotEquals(oldLeader, newLeaderId);
        return newLeaderId;
    }

    int findLeaderOfMajority() {
        N node = Arrays.stream(nodes).sorted(Comparator.comparingInt(this::getLeaderIdOf))
                .toList()
                .get(nodes.length / 2);
        return getLeaderIdOf(node);
    }

    void assertLeaderOfMajority(int majorityLeaderId) {
        assertEquals(majorityLeaderId, findLeaderOfMajority());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(N::shutdown);
        transport.shutdown();
        TIMER.shutdown();
        RNG = new Random(SEED);
    }

    static Random getRNG() {
        return RNG;
    }
}
