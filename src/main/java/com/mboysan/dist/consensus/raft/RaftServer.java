package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.State.Role.*;

public class RaftServer implements RaftRPC, AutoCloseable {

    private final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

    private static final long SEED = 1L;
    static {
        System.out.println("SEED=" + SEED);
    }
    private final static Random RNG = new Random(SEED);

    private final ExecutorService executorQueue = Executors.newCachedThreadPool();

    private static long ELECTION_TIMEOUT = 10000;
    private static long RPC_TIMEOUT = 10000;
    private static final long ELECTION_TIMER_TIMEOUT_MS = 5000;

    private final ScheduledExecutorService electionTimer = Executors.newSingleThreadScheduledExecutor();

    private final Transport transport;
    private final int nodeId;

    final State state = new State();
    final Map<Integer, Peer> peers = new HashMap<>();

    public RaftServer(int nodeId, Transport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        transport.addServer(nodeId, this);

//        scheduleElectionTimer();
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

    private void forEachPeerParallel(Consumer<Peer> peerConsumer) {
        List<Future<?>> futures = new ArrayList<>();
        peers.forEach((id, peer) -> {
            Future<?> future = executorQueue.submit(() -> {
                peerConsumer.accept(peer);
            });
            futures.add(future);
        });
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error(e.getMessage(), e);
            } catch (ExecutionException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public synchronized void close() {
        executorQueue.shutdown();
        electionTimer.shutdown();
        transport.removeServer(nodeId);
        peers.clear();
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    private synchronized void onElectionTimeout() {
        if (!state.isElectionNeeded) {
            state.isElectionNeeded = true;    // it will be needed in the next iteration
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
        if (state.role == FOLLOWER || state.role == CANDIDATE && state.isElectionNeeded) {
            state.isElectionNeeded = false;
            state.currentTerm++;
            state.votedFor = nodeId;
            state.role = CANDIDATE;

            // reset all state for peers
            peers.forEach((i, peer) -> peer.reset());
        }
    }

    private void sendRequestVoteToPeers() {
        if (state.role == CANDIDATE) {
            final Object lock = new Object();

            int currentTerm = state.currentTerm;
            int lastLogTerm = logTerm(state.raftLog.size());
            int lastLogIndex = state.raftLog.size();
            forEachPeerParallel((peer) -> {
                RequestVoteRequest request = new RequestVoteRequest(currentTerm, nodeId, lastLogIndex, lastLogTerm)
                        .setSenderId(nodeId)
                        .setReceiverId(peer.peerId);
                RequestVoteResponse response = getRPC(transport).requestVote(request);

                synchronized (lock) {
                    if (state.currentTerm < response.getTerm()) {
                        stepDown(response.getTerm());
                    }
                    if (state.role == CANDIDATE && state.currentTerm == response.getTerm()) {
                        peer.rpcDue = Long.MAX_VALUE; //TODO: revise
                        peer.voteGranted = response.isVoteGranted();
                    }
                }
            });
        }
    }

    private void becomeLeader() {
        int voteCount = peers.values().stream().mapToInt(peer -> peer.voteGranted ? 1 : 0).sum();
        if (state.role == CANDIDATE && (voteCount + 1) > peers.size() / 2) {
            state.role = LEADER;
            state.leaderId = nodeId;
            peers.forEach((peerId, peer) -> {
                peer.nextIndex = state.raftLog.size() + 1;
            });
        }
    }

    private void sendAppendEntriesToPeers() {
        if (state.role == LEADER) {
            final Object lock = new Object();

            List<LogEntry> raftLog = List.copyOf(state.raftLog);
            int logSize = raftLog.size();
            int leaderId = state.leaderId;
            int currentTerm = state.currentTerm;
            int commitIndex = state.commitIndex;
            forEachPeerParallel((peer) -> {
                if (peer.matchIndex < logSize) {
                    peer.rpcDue = System.currentTimeMillis() + ELECTION_TIMEOUT / 2;    //TODO: revise
                    int prevIndex = peer.nextIndex - 1;
                    int lastIndex = Math.min(prevIndex, logSize);
                    peer.nextIndex = lastIndex;
                    int prevTerm = logTerm(prevIndex);
                    List<LogEntry> entries = raftLog.subList(peer.nextIndex, lastIndex);

                    AppendEntriesRequest request = new AppendEntriesRequest(
                            currentTerm, leaderId, prevIndex, prevTerm, entries, commitIndex)
                            .setSenderId(nodeId)
                            .setReceiverId(peer.peerId);
                    AppendEntriesResponse response = getRPC(transport).appendEntries(request);

                    synchronized (lock) {
                        if (state.currentTerm < response.getTerm()) {
                            stepDown(response.getTerm());
                        } else if (state.role == LEADER && state.currentTerm == response.getTerm()) {
                            if (response.isSuccess()) {
                                peer.matchIndex = response.getMatchIndex();
                                peer.nextIndex = response.getMatchIndex() + 1;
                            } else {
                                peer.nextIndex = Math.max(1, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                            }
                        }
                    }
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (state.role == LEADER) {
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
            if (state.role == LEADER && logTerm(state.lastApplied) == state.currentTerm) {
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
            state.isElectionNeeded = false;
        } else {
            granted = false;
        }
        return new RequestVoteResponse(state.currentTerm, granted).responseTo(request);
    }

    @Override
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (state.currentTerm < request.getTerm()) {
            stepDown(request.getTerm());
        }
        if (state.currentTerm > request.getTerm()) {
            return new AppendEntriesResponse(state.currentTerm, false).responseTo(request);
        } else {
            state.leaderId = request.getLeaderId();
            state.role = FOLLOWER;
            state.isElectionNeeded = false;
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
        if (state.role == LEADER) {
            state.raftLog.push(new LogEntry(request.getCommand(), state.currentTerm));
            return new StateMachineResponse(true).responseTo(request);
        }
        if (state.leaderId == -1) {
            throw new IllegalStateException("leader unresolved");
        }

        return getRPC(transport).stateMachineRequest(request)
                .setReceiverId(state.leaderId)
                .setSenderId(nodeId);   // TODO: investigate
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private synchronized void stepDown(int newTerm) {
        state.currentTerm = newTerm;
        state.role = FOLLOWER;
        state.votedFor = -1;
        state.isElectionNeeded = false;
    }

    private synchronized int logTerm(int index) {
        if (index < 1 || index > state.raftLog.size()) {
            return 0;
        }
        return state.raftLog.get(index - 1).getTerm();
    }

    /*----------------------------------------------------------------------------------
     * Inner Enum(s) / Class(es)
     * ----------------------------------------------------------------------------------*/

    private static class Peer {
        final int peerId;
        long rpcDue;
        boolean voteGranted;
        /** for each server, index of the next log entry to send to that server (initialized to leader last
         * log index + 1) */
        int matchIndex;
        /** for each server, index of highest log entry known to be replicated on server (initialized to 0,
         * increases monotonically) */
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
