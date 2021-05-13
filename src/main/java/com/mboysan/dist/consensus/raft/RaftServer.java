package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.RaftServer.ServerRole.*;

public class RaftServer implements RaftRPC {

    private static long ELECTION_TIMEOUT = 10000;
    private static long RPC_TIMEOUT = 10000;

    private static final long SEED = 1L;
    static {
        System.out.println("SEED=" + SEED);
    }
    private final static Random RNG = new Random(SEED);

    final Transport transport;

    enum ServerRole {
        CANDIDATE, FOLLOWER, LEADER
    }

    private final State state = new State();
    private ServerRole serverRole = CANDIDATE;

    private static final long ELECTION_TIMER_TIMEOUT_MS = 5000;
    private final ScheduledExecutorService electionTimer = Executors.newSingleThreadScheduledExecutor();
    private boolean isElectionNeeded = true;


    final int nodeId;
    int leaderId = -1;

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

    private final Map<Integer, Peer> peers = new HashMap<>();

    public synchronized void onServerAdded(int nodeId) {
        peers.put(nodeId, new Peer(nodeId));
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
        if (serverRole == FOLLOWER || serverRole == CANDIDATE && isElectionNeeded) {
            isElectionNeeded = false;
            state.currentTerm++;
            state.votedFor = nodeId;
            serverRole = CANDIDATE;

            // reset all state for peers
            peers.forEach((i, peer) -> peer.reset());
        }
    }

    private void sendRequestVoteToPeers() {
        if (serverRole == CANDIDATE) {
            peers.forEach((peerId, peer) -> {
                transport.send(nodeId, peerId, protoClient -> {
                    RaftClient raftClient = (RaftClient) protoClient;

                    int currentTerm = state.currentTerm;
                    int lastLogTerm = logTerm(state.raftLog.size());
                    int lastLogIndex = state.raftLog.size();
                    RequestVoteRequest request = new RequestVoteRequest(currentTerm, nodeId, lastLogIndex, lastLogTerm);
                    RequestVoteResponse response = raftClient.requestVote(request);

                    if (state.currentTerm < response.getTerm()) {
                        stepDown(response.getTerm());
                    }
                    if (serverRole == CANDIDATE && state.currentTerm == response.getTerm()) {
                        peer.rpcDue = Long.MAX_VALUE; //TODO: revise
                        peer.voteGranted = response.isVoteGranted();
                    }
                });
            });
        }
    }

    private void becomeLeader() {
        int voteCount = peers.values().stream().mapToInt(peer -> peer.voteGranted ? 1 : 0).sum();
        if (serverRole == CANDIDATE && (voteCount + 1) > peers.size() / 2) {
            serverRole = LEADER;
            leaderId = nodeId;
            peers.forEach((peerId, peer) -> {
                peer.nextIndex = state.raftLog.size() + 1;
            });
        }
    }

    private void sendAppendEntriesToPeers() {
        if (serverRole == LEADER) {
            peers.forEach((peerId, peer) -> {
                if (peer.matchIndex < state.raftLog.size()) {
                    peer.rpcDue = System.currentTimeMillis() + ELECTION_TIMEOUT / 2;    //TODO: revise
                    int prevIndex = peer.nextIndex - 1;
                    int lastIndex = Math.min(prevIndex, state.raftLog.size());
                    peer.nextIndex = lastIndex;
                    int prevTerm = logTerm(prevIndex);

                    List<LogEntry> entries = state.raftLog.subList(peer.nextIndex, lastIndex);

                    transport.send(nodeId, peerId, protoClient -> {
                        RaftClient raftClient = (RaftClient) protoClient;

                        AppendEntriesRequest request = new AppendEntriesRequest(
                                state.currentTerm, leaderId, prevIndex, prevTerm, entries, state.commitIndex);
                        AppendEntriesResponse response = raftClient.appendEntries(request);

                        if (state.currentTerm < response.getTerm()) {
                            stepDown(response.getTerm());
                        } else if (serverRole == LEADER && state.currentTerm == response.getTerm()) {
                            if (response.isSuccess()) {
                                peer.matchIndex = response.getMatchIndex();
                                peer.nextIndex = response.getMatchIndex() + 1;
                            } else {
                                peer.nextIndex = Math.max(1, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                            }
                        }
                    });
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (serverRole == LEADER) {
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
            if (serverRole == LEADER && logTerm(state.lastApplied) == state.currentTerm) {
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
        return new RequestVoteResponse(state.currentTerm, granted);
    }

    @Override
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (state.currentTerm < request.getTerm()) {
            stepDown(request.getTerm());
        }
        if (state.currentTerm > request.getTerm()) {
            return new AppendEntriesResponse(state.currentTerm, false);
        } else {
            leaderId = request.getLeaderId();
            serverRole = FOLLOWER;
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
                return new AppendEntriesResponse(state.currentTerm, true, index);
            } else {
                return new AppendEntriesResponse(state.currentTerm, false);
            }
        }
    }

    @Override
    public synchronized boolean stateMachineRequest(String clientCommand) {
        if (serverRole == LEADER) {
            state.raftLog.push(new LogEntry(clientCommand, state.currentTerm));
            return true;
        }
        if (leaderId == -1) {
            throw new IllegalStateException("leader unresolved");
        }
        AtomicBoolean result = new AtomicBoolean(false);
        transport.send(nodeId, leaderId, protoRPC -> {
            RaftClient raftClient = (RaftClient) protoRPC;
            result.set(raftClient.stateMachineRequest(clientCommand));
        });
        return result.get();
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    void stepDown(int newTerm) {
        state.currentTerm = newTerm;
        serverRole = FOLLOWER;
        state.votedFor = -1;
        isElectionNeeded = false;
    }

    int logTerm(int index) {
        if (index < 1 || index > state.raftLog.size()) {
            return 0;
        }
        return state.raftLog.get(index - 1).getTerm();
    }

}
