package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import com.mboysan.consensus.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;

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
    void advanceTimeForElections() throws InterruptedException {
        // use fake timer
        // following loop should allow triggering election on the node with slowest electionTimer
        TIMER.advance(advanceTimeInterval * 2);
    }

    int assertOneLeader() {
        int leaderId = -1;
        for (BizurNode node : nodes) {
            if (leaderId == -1) {
//                leaderId = node.getState().leaderId;
            }
//            assertEquals(leaderId, node.getState().leaderId);
        }
        assertNotEquals(-1, leaderId);
        return leaderId;
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
