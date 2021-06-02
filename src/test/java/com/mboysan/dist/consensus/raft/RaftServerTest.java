package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class RaftServerTest {

    private final static int SERVER_COUNT = 3;
    private InVMTransport transport;
    private final RaftServer[] servers = new RaftServer[SERVER_COUNT];
    private final ExecutorService raftServerExecutor = Executors.newFixedThreadPool(SERVER_COUNT);
    private int leaderId = -1;
    private int next = -1;

    @BeforeEach
    void setUp() {
        Set<Integer> serverIds = new HashSet<>();
        transport = new InVMTransport();
        for (int i = 0; i < SERVER_COUNT; i++) {
            RaftServer server = new RaftServer(i, transport);
            servers[i] = server;
            serverIds.add(server.getNodeId());
            raftServerExecutor.submit(server);
        }
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
    void tearDown() throws Exception {
        System.out.println("TEARING DOWN");
        raftServerExecutor.shutdown();
        for (RaftServer server : servers) {
            server.close();
        }
        transport.close();
        for (RaftServer server : servers) {
            assertEquals(0, server.peers.keySet().size());
        }
        System.out.println("TEARED DOWN");
    }

    @Test
    void tesst() {

    }

    @Test
    void testSimple() throws InterruptedException {
        System.out.println("killing");

        int id = getNextNonLeader().getNodeId();
        transport.kill(id);

        Thread.sleep(6000);

        transport.revive(id);

        Thread.sleep(6000);

        System.out.println("test should've ended");
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

    private static class RaftServerForTesting extends RaftServer {
        public RaftServerForTesting(int nodeId, Transport transport) {
            super(nodeId, transport);
        }
        synchronized void assertState(State expectedState) {
            assertEquals(expectedState, state);
        }
        synchronized int getLeaderId() {
            return state.leaderId;
        }
    }

}
