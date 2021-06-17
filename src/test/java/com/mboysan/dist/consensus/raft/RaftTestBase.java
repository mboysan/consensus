package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import com.mboysan.util.Timers;
import com.mboysan.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class RaftTestBase {

    static boolean USE_REAL_TIMER = true;
    private static final TimersForTesting TIMER = new TimersForTesting();

    RaftServer[] nodes;
    private InVMTransport transport;
    private long advanceTimeInterval = -1;
    boolean skipTeardown = false;

    @BeforeEach
    void setUp() {
        skipTeardown = false;
    }

    void init(int numServers) throws InterruptedException, ExecutionException {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new RaftServer[numServers];
        transport = new InVMTransport();
        for (int i = 0; i < numServers; i++) {
            RaftServer node;
            if (USE_REAL_TIMER) {
                node = new RaftServer(i, transport);
            } else {
                node = new RaftServerForTesting(i, transport);
            }
            nodes[i] = node;
            futures.add(node.start());

            if (node.electionTimeoutMs > advanceTimeInterval) {
                advanceTimeInterval = node.electionTimeoutMs;   // find max amount to wait
            }
        }
        advanceTimeInterval += 100; // add 100 ms just in case

        advanceTimeForElections();
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    /**
     * Advances time to try triggering election on all nodes.
     */
    void advanceTimeForElections() throws InterruptedException {
        if (USE_REAL_TIMER) {
            Thread.sleep(advanceTimeInterval * 2);
        } else {
            // use fake timer
//            TIMER.runAll();
            TIMER.runAll();
            TIMER.runAll();
            TIMER.runAll(); // this should allow triggering election on the node with slowest electionTimer
        }
    }

    void disconnect(int nodeId) {
        transport.kill(nodeId);
    }

    void connect(int nodeId) {
        transport.revive(nodeId);
    }

    void kill(int nodeId) {
        TIMER.pause("updateTimer-node" + nodeId);
        disconnect(nodeId);
    }

    void revive(int nodeId) {
        TIMER.resume("updateTimer-node" + nodeId);
        connect(nodeId);
    }

    int assertLeaderChanged(int oldLeader, boolean isChangeVisibleOnOldLeader) {
        int newLeaderId;
        if (isChangeVisibleOnOldLeader) {
            newLeaderId = assertOneLeader();
        } else {
            newLeaderId = -1;
            for (RaftServer node : nodes) {
                if (node.getNodeId() == oldLeader) {
                    // the old leader should still think its the leader
                    assertEquals(oldLeader, node.state.leaderId);
                } else {
                    if (newLeaderId == -1) {
                        newLeaderId = node.state.leaderId;
                    }
                    assertEquals(newLeaderId, node.state.leaderId);
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
        for (RaftServer node : nodes) {
            if (leaderId == -1) {
                leaderId = node.state.leaderId;
            }
            assertEquals(leaderId, node.state.leaderId);
        }
        assertNotEquals(-1, leaderId);
        return leaderId;
    }

    void assertLogsEquals(List<String> commands) {
        RaftLog log0 = nodes[0].state.raftLog;
        assertEquals(commands.size(), log0.size());
        for (int i = 0; i < nodes[0].state.raftLog.size(); i++) {
            assertEquals(commands.get(i), log0.get(i).getCommand());
        }
        for (RaftServer server : nodes) {
            assertEquals(log0, server.state.raftLog);
        }
    }

    @AfterEach
    void tearDown() {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(RaftServer::shutdown);
        transport.shutdown();
        TIMER.shutdown();
    }

    private static class RaftServerForTesting extends RaftServer {
        public RaftServerForTesting(int nodeId, Transport transport) {
            super(nodeId, transport);
        }
        @Override
        Timers createTimers() {
            return TIMER;
        }
    }
}
