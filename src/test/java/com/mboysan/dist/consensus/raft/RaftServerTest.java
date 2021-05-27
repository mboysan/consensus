package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.State.Role.CANDIDATE;
import static com.mboysan.dist.consensus.raft.State.Role.FOLLOWER;
import static org.junit.jupiter.api.Assertions.*;

class RaftServerTest {

    private final static int SERVER_COUNT = 3;
    Transport transport;
    RaftServer[] servers = new RaftServer[SERVER_COUNT];

    @BeforeEach
    void setUp() {
        Set<Integer> serverIds = new HashSet<>();
        transport = new InVMTransport();
        for (int i = 0; i < SERVER_COUNT; i++) {
            RaftServer server = new RaftServer(i, transport);
            servers[i] = server;
            serverIds.add(server.getNodeId());
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
    void testRequestVoteRPC_whenInit_thenGranted() {
        RaftServer server0 = servers[0];
        server0.startNewElection();

        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        RaftServer server1 = servers[1];
        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server0.state.currentTerm, response.getTerm());
        assertTrue(response.isVoteGranted());

        assertEquals(server0.state.currentTerm, server1.state.currentTerm);
        assertEquals(FOLLOWER, server1.state.role);
        assertEquals(server0.getNodeId(), server1.state.votedFor);
        assertFalse(server1.state.isElectionNeeded);
    }

    @Test
    void testRequestVoteRPC_whenReceiverTermGreater_thenNotGranted() {
        RaftServer server0 = servers[0];
        server0.startNewElection();

        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        RaftServer server1 = servers[1];
        server1.state.currentTerm = 2;

        State expectedServer1State = new State();
        copyState(expectedServer1State, server1.state);

        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server1.state.currentTerm, response.getTerm());
        assertFalse(response.isVoteGranted());

        // state must not be changed
        assertEquals(expectedServer1State, server1.state);
    }

    @Test
    void testRequestVoteRPC_whenTermEqualsButVotedForAnother_thenNotGranted() {
        RaftServer server0 = servers[0];
        server0.startNewElection();

        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        RaftServer server1 = servers[1];
        server1.state.currentTerm = 1;
        server1.state.votedFor = servers[2].getNodeId();

        State expectedServer1State = new State();
        copyState(expectedServer1State, server1.state);

        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server1.state.currentTerm, response.getTerm());
        assertFalse(response.isVoteGranted());

        // state must not be changed
        assertEquals(expectedServer1State, server1.state);
    }

    @Test
    void testRequestVoteRPC_whenReceiverLogMoreUpToDate_thenNotGranted() {
        RaftServer server0 = servers[0];
        server0.startNewElection();

        RaftServer server1 = servers[1];
        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        server1.state.currentTerm = 1;
        RaftLog logS1 = new RaftLog();
        logS1.push(new LogEntry("", 3));
        server1.state.raftLog = logS1;
        server1.state.votedFor = servers[2].getNodeId();

        State expectedServer1State = new State();
        copyState(expectedServer1State, server1.state);

        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server1.state.currentTerm, response.getTerm());
        assertFalse(response.isVoteGranted());

        // state must not be changed
        assertEquals(expectedServer1State, server1.state);
    }

    @Test
    void testRequestVoteRPC_whenReceiverLastLogTermSmaller_thenGranted() {
/*        RaftServer server0 = servers[0];
        server0.startNewElection();

        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        RaftServer server1 = servers[1];
        server1.state.currentTerm = server0.state.currentTerm;
        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server0.state.currentTerm, response.getTerm());
        assertTrue(response.isVoteGranted());

        assertEquals(server0.state.currentTerm, server1.state.currentTerm);
        assertEquals(FOLLOWER, server1.state.role);
        assertEquals(server0.getNodeId(), server1.state.votedFor);
        assertFalse(server1.state.isElectionNeeded);



        RaftServer server0 = servers[0];
        server0.startNewElection();

        RaftServer server1 = servers[1];
        RequestVoteRequest request = new RequestVoteRequest(
                server0.state.currentTerm,
                server0.getNodeId(),
                server0.state.raftLog.lastLogIndex(),
                server0.state.raftLog.lastLogTerm());
        server1.state.currentTerm = 1;
        RaftLog logS1 = new RaftLog();
        logS1.push(new LogEntry("", 3));
        server1.state.raftLog = logS1;
        server1.state.votedFor = servers[2].getNodeId();

        State expectedServer1State = new State();
        copyState(expectedServer1State, server1.state);

        RequestVoteResponse response = server1.requestVote(request);
        assertEquals(server1.state.currentTerm, response.getTerm());
        assertFalse(response.isVoteGranted());

        // state must not be changed
        assertEquals(expectedServer1State, server1.state);*/
    }

    private void setState(RaftServer server, State newState) {
        copyState(server.state, newState);
    }

    private static void copyState(State oldState, State newState) {
        oldState.raftLog = newState.raftLog.copy();
        oldState.currentTerm = newState.currentTerm;
        oldState.lastApplied = newState.lastApplied;
        oldState.votedFor = newState.votedFor;
        oldState.leaderId = newState.leaderId;
        oldState.isElectionNeeded = newState.isElectionNeeded;
        oldState.role = newState.role;
    }

}