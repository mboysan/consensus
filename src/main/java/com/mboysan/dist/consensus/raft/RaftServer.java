package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.RaftServer.Role.*;

public class RaftServer implements RaftRPC {

    private static final long SEED = 1L;
    static {
        System.out.println("SEED=" + SEED);
    }
    private final static Random RNG = new Random(SEED);


    private static long ELECTION_TIMEOUT = 10000;
    private static long RPC_TIMEOUT = 10000;
    private static final long ELECTION_TIMER_TIMEOUT_MS = 5000;

    private final ScheduledExecutorService electionTimer = Executors.newSingleThreadScheduledExecutor();

    private final Transport transport;

    private final State state = new State();
    private Role role = CANDIDATE;

    private boolean isElectionNeeded = true;

    private final int nodeId;
    private int leaderId = -1;

    private final Map<Integer, Peer> peers = new HashMap<>();

    public RaftServer(int nodeId, Transport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        transport.addServer(nodeId, this);

        scheduleElectionTimer();
    }

    void scheduleElectionTimer() {
        long electionTimeoutMs = RNG.nextInt((int) (ELECTION_TIMER_TIMEOUT_MS/1000) + 1) * 1000;
        System.out.println("Election timeout for node=" + nodeId + " is " + electionTimeoutMs);
        electionTimer.scheduleAtFixedRate(() -> onElectionTimeout(), 0, electionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void onServerListChanged(Set<Integer> serverIds) {
        // first, we add new peers for each new serverId
        serverIds.forEach(nodeId -> {
            peers.putIfAbsent(nodeId, new Peer(nodeId));
        });

        // next, we remove all peers who are not in the serverIds set.
        Set<Integer> difference = new HashSet<>(peers.keySet());
        difference.removeAll(serverIds);
        peers.keySet().removeAll(difference);
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    private synchronized void onElectionTimeout() {
        if (!isElectionNeeded) {
            isElectionNeeded = true;    // it will be needed in the next iteration
        } else {
            update();
        }
    }

    private synchronized void update() {
        startNewElection();
        sendRequestVoteToPeers();
        becomeLeader();
        sendAppendEntriesToPeers();
    }

    private void startNewElection() {
        if (role == FOLLOWER || role == CANDIDATE && isElectionNeeded) {
            isElectionNeeded = false;
            state.currentTerm++;
            state.votedFor = nodeId;
            role = CANDIDATE;

            // reset all state for peers
            peers.forEach((i, peer) -> peer.reset());
        }
    }

    private void sendRequestVoteToPeers() {
        if (role == CANDIDATE) {
            peers.forEach((peerId, peer) -> {
                int currentTerm = state.currentTerm;
                int lastLogTerm = logTerm(state.raftLog.size());
                int lastLogIndex = state.raftLog.size();
                RequestVoteRequest request = new RequestVoteRequest(currentTerm, nodeId, lastLogIndex, lastLogTerm)
                        .setSenderId(nodeId)
                        .setReceiverId(peerId);
                RequestVoteResponse response = getRPC(transport).requestVote(request);

                if (state.currentTerm < response.getTerm()) {
                    stepDown(response.getTerm());
                }
                if (role == CANDIDATE && state.currentTerm == response.getTerm()) {
                    peer.rpcDue = Long.MAX_VALUE; //TODO: revise
                    peer.voteGranted = response.isVoteGranted();
                }
            });
        }
    }

    private void becomeLeader() {
        int voteCount = peers.values().stream().mapToInt(peer -> peer.voteGranted ? 1 : 0).sum();
        if (role == CANDIDATE && (voteCount + 1) > peers.size() / 2) {
            role = LEADER;
            leaderId = nodeId;
            peers.forEach((peerId, peer) -> {
                peer.nextIndex = state.raftLog.size() + 1;
            });
        }
    }

    private void sendAppendEntriesToPeers() {
        if (role == LEADER) {
            peers.forEach((peerId, peer) -> {
                if (peer.matchIndex < state.raftLog.size()) {
                    peer.rpcDue = System.currentTimeMillis() + ELECTION_TIMEOUT / 2;    //TODO: revise
                    int prevIndex = peer.nextIndex - 1;
                    int lastIndex = Math.min(prevIndex, state.raftLog.size());
                    peer.nextIndex = lastIndex;
                    int prevTerm = logTerm(prevIndex);

                    List<LogEntry> entries = state.raftLog.subList(peer.nextIndex, lastIndex);

                    AppendEntriesRequest request = new AppendEntriesRequest(
                            state.currentTerm, leaderId, prevIndex, prevTerm, entries, state.commitIndex)
                            .setSenderId(nodeId)
                            .setReceiverId(peerId);
                    AppendEntriesResponse response = getRPC(transport).appendEntries(request);

                    if (state.currentTerm < response.getTerm()) {
                        stepDown(response.getTerm());
                    } else if (role == LEADER && state.currentTerm == response.getTerm()) {
                        if (response.isSuccess()) {
                            peer.matchIndex = response.getMatchIndex();
                            peer.nextIndex = response.getMatchIndex() + 1;
                        } else {
                            peer.nextIndex = Math.max(1, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                        }
                    }
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (role == LEADER) {
            int majorityIdx = peers.size() / 2;
            int n = IntStream.concat(
                    peers.values().stream().flatMapToInt(peer -> IntStream.of(peer.matchIndex)),
                    IntStream.of(state.raftLog.size())) // append len(log)
                    .sorted()
                    .boxed().collect(Collectors.toList()).get(majorityIdx);
            if (logTerm(n) == state.currentTerm) {
                state.commitIndex = n;
            }
        }
    }

    private void advanceStateMachine() {
        if (state.lastApplied < state.commitIndex) {
            state.lastApplied++;
            Serializable result = stateMachineApply(state.raftLog.get(state.lastApplied).getCommand());
            if (role == LEADER && logTerm(state.lastApplied) == state.currentTerm) {
                // send result to client
            }
        }
    }

    Serializable stateMachineApply(Serializable command) {
        return "APPLIED";
    }

    /*----------------------------------------------------------------------------------
     * RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        boolean granted;
        if (state.currentTerm < request.getTerm()) {
            stepDown(request.getTerm());
        }
        if (state.currentTerm == request.getTerm() &&
                (state.votedFor == -1 || state.votedFor == request.getCandidateId()) &&
                (request.getLastLogTerm() >= logTerm(state.raftLog.size())) ||
                ((request.getLastLogTerm() == logTerm(state.raftLog.size())) && (request.getLastLogIndex() >= state.raftLog.size()))
        ) {
            granted = true;
            state.votedFor = request.getCandidateId();
            isElectionNeeded = false;
        } else {
            granted = false;
        }
        return new RequestVoteResponse(state.currentTerm, granted).responseTo(request);
        /*return new RequestVoteResponse()
                .setTerm(state.currentTerm)
                .setVoteGranted(granted)
                .responseTo(request);*/
    }

    @Override
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (state.currentTerm < request.getTerm()) {
            stepDown(request.getTerm());
        }
        if (state.currentTerm > request.getTerm()) {
            return new AppendEntriesResponse(state.currentTerm, false).responseTo(request);
        } else {
            leaderId = request.getLeaderId();
            role = FOLLOWER;
            isElectionNeeded = false;
            boolean success = (request.getPrevLogIndex() == 0) ||
                    (request.getPrevLogIndex() <= state.raftLog.size() &&
                            logTerm(request.getPrevLogIndex()) == request.getPrevLogTerm());
            int index;
            if (success) {
                index = request.getPrevLogIndex();
                for (int i = 0; i < request.getEntries().size(); i++) {
                    index++;
                    if (logTerm(index) != request.getEntries().get(i).getTerm()) {
                        while (state.raftLog.size() > index - 1) {
                            state.raftLog.pop();
                        }
                        state.raftLog.push(request.getEntries().get(i));
                    }
                }
                state.commitIndex = Math.max(state.commitIndex, request.getLeaderCommit());
                return new AppendEntriesResponse(state.currentTerm, true, index).responseTo(request);
            } else {
                return new AppendEntriesResponse(state.currentTerm, false).responseTo(request);
            }
        }
    }

    @Override
    public synchronized StateMachineResponse stateMachineRequest(StateMachineRequest request) {
        if (role == LEADER) {
            state.raftLog.push(new LogEntry(request.getCommand(), state.currentTerm));
            return new StateMachineResponse(true).responseTo(request);
        }
        if (leaderId == -1) {
            throw new IllegalStateException("leader unresolved");
        }

        return getRPC(transport).stateMachineRequest(request)
                .setReceiverId(leaderId)
                .setSenderId(nodeId);   // TODO: investigate
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    void stepDown(int newTerm) {
        state.currentTerm = newTerm;
        role = FOLLOWER;
        state.votedFor = -1;
        isElectionNeeded = false;
    }

    int logTerm(int index) {
        if (index < 1 || index > state.raftLog.size()) {
            return 0;
        }
        return state.raftLog.get(index - 1).getTerm();
    }

    /*----------------------------------------------------------------------------------
     * Inner Enum(s) / Class(es)
     * ----------------------------------------------------------------------------------*/

    enum Role {
        CANDIDATE, FOLLOWER, LEADER
    }

    private static class Peer {
        final int peerId;
        long rpcDue;
        boolean voteGranted;
        int matchIndex;
        int nextIndex;

        private Peer(int peerId) {
            this.peerId = peerId;
            reset();
        }
        void reset() {
            rpcDue = 0;
            voteGranted = false;
            matchIndex = 0;
            nextIndex = 1;
        }
    }

}
