package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import com.mboysan.util.Timers;
import com.mboysan.util.TimersForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class RaftServerTest {

    private static final TimersForTesting TIMER = new TimersForTesting();

    private final static int SERVER_COUNT = 3;
    private InVMTransport transport;
    private final RaftServerForTesting[] servers = new RaftServerForTesting[SERVER_COUNT];
    private final ExecutorService raftServerExecutor = Executors.newFixedThreadPool(SERVER_COUNT);
    private int leaderId = -1;
    private int next = -1;

    @BeforeEach
    void setUp() {
        Set<Integer> serverIds = new HashSet<>();
        transport = new InVMTransport();
        for (int i = 0; i < SERVER_COUNT; i++) {
            RaftServerForTesting server = new RaftServerForTesting(i, transport);
            servers[i] = server;
            serverIds.add(server.getNodeId());
            server.run();
//            raftServerExecutor.submit(server);
        }

        TIMER.runAll();

        for (RaftServer server : servers) {
            assertFalse(server.peers.containsKey(server.getNodeId()));
            Set<Integer> idsExceptSelf = new HashSet<>(serverIds);
            idsExceptSelf.remove(server.getNodeId());
            assertTrue(server.peers.keySet().containsAll(idsExceptSelf));

            while(server.state.leaderId == -1) {}  //fixme: busy wait
            if (leaderId == -1) {
                leaderId = server.state.leaderId;
            } else {
                assertEquals(leaderId, server.state.leaderId);
            }
        }
    }

    @AfterEach
    void tearDown() {
        raftServerExecutor.shutdown();
        for (RaftServer server : servers) {
            server.shutdown();
        }
        transport.shutdown();
        for (RaftServer server : servers) {
            assertEquals(0, server.peers.keySet().size());
        }
    }

    @Test
    void testFollowerFailure() throws IOException, InterruptedException {
        RaftServer follower1 = getNextNonLeader();
        assertTrue(newEntry(follower1, "cmd1"));

        TIMER.runAll();
        TIMER.runAll();

        int id = follower1.getNodeId();
        transport.kill(id);

        TIMER.runAll();
        TIMER.runAll();

        RaftServer follower2 = getNextNonLeader();
        assertTrue(newEntry(follower2, "cmd2"));

        TIMER.runAll();
        TIMER.runAll();

        transport.revive(id);

        TIMER.runAll();
        TIMER.runAll();

        System.out.println();
    }

    @Test
    void testLeaderFailure() throws IOException {
        RaftServer leader = getLeader();
        assertTrue(newEntry(leader, "cmd1"));

        TIMER.runAll();

        int id = leader.getNodeId();
        transport.kill(id);

        TIMER.runAll();

        RaftServer follower = getNextNonLeader();
        assertFalse(newEntry(follower, "cmd2"));

        TIMER.runAll();

        transport.revive(id);

        TIMER.runAll();

        System.out.println();
    }

    private RaftServer getLeader() {
        return servers[leaderId];
    }

    private RaftServer getNextNonLeader() {
        next = ++next % servers.length;
        if (next == leaderId) {
            return getNextNonLeader();
        }
        return servers[next];
    }

    private boolean newEntry(RaftServer server, String entry) throws IOException {
        return server.stateMachineRequest(new StateMachineRequest(entry)).isApplied();
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
