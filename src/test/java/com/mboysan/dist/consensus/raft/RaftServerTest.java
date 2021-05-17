package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class RaftServerTest {

    private final static int SERVER_COUNT = 3;
    Transport transport;
    RaftServer[] servers = new RaftServer[SERVER_COUNT];
    Set<Integer> serverIds = new HashSet<>();

    @BeforeEach
    void setUp() {
        transport = new InVMTransport();
        for (int i = 0; i < SERVER_COUNT; i++) {
            servers[i] = new RaftServer(i, transport);
            serverIds.add(i);
        }
        for (RaftServer server : servers) {
            assertTrue(server.peers.keySet().containsAll(serverIds));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (RaftServer server : servers) {
            server.close();
        }
        transport.close();
        for (RaftServer server : servers) {
            assertEquals(0, server.peers.keySet().size());
        }
    }

    @Test
    void testRequestVoteRPC() {

    }

    private void setState(RaftServer server, State newState) {
        server.state.raftLog = newState.raftLog;
        server.state.currentTerm = newState.currentTerm;
        server.state.lastApplied = newState.lastApplied;
        server.state.votedFor = newState.votedFor;
        server.state.leaderId = newState.leaderId;
        server.state.isElectionNeeded = newState.isElectionNeeded;
        server.state.role = newState.role;
    }

}