package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;
import com.mboysan.util.TimerQueue;
import com.mboysan.util.Timers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.State.Role.*;

public class RaftServer implements Runnable, RaftRPC, AutoCloseable {

    private final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

    private static final long SEED = 1L;
    static {
        System.out.println("SEED=" + SEED);
    }
    private final static Random RNG = new Random(SEED);

    private final ExecutorService executorQueue = Executors.newCachedThreadPool();

    private static final long ELECTION_TIMEOUT_MS = 5000;
    private final Timers timers = new TimerQueue();

    private volatile boolean isRunning;

    private final Transport transport;
    private final int nodeId;

    final State state = new State();
    final Map<Integer, Peer> peers = new HashMap<>();

    public RaftServer(int nodeId, Transport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        transport.addServer(nodeId, this);
        isRunning = true;
    }

    public int getNodeId() {
        return nodeId;
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
            if (id != nodeId) {
                futures.add(executorQueue.submit(() -> peerConsumer.accept(peer)));
            } else {
                // we don't send the request to self.
//                peerConsumer.accept(peer);
            }
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
        isRunning = false;
        timers.shutdown();
        executorQueue.shutdown();
        transport.removeServer(nodeId);
        peers.clear();
    }

    @Override
    public void run() {
        scheduleTimers();
        /*while (isRunning) {
            update();
        }*/
    }

    void scheduleTimers() {
        long electionTimeoutMs = RNG.nextInt((int) (ELECTION_TIMEOUT_MS /1000) + 1) * 1000;
        timers.schedule("electionTimer-node" + nodeId, this::onElectionTimeout, electionTimeoutMs, electionTimeoutMs);

        long updateTimeoutMs = electionTimeoutMs / 2;
        timers.schedule("updateTimer-node" + nodeId, this::update, updateTimeoutMs, updateTimeoutMs);
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    private synchronized void onElectionTimeout() {
        LOG.info("isElectionNeeded={}", state.isElectionNeeded);
        if (!state.isElectionNeeded) {
            state.isElectionNeeded = true;
            LOG.info("node {} election timeout.", nodeId);
        }
    }

    private synchronized void update() {
        startNewElection();
        sendRequestVoteToPeers();
        becomeLeader();
        sendAppendEntriesToPeers();
        advanceCommitIndex();
        advanceStateMachine();
    }

    void startNewElection() {
        if ((state.role == FOLLOWER || state.role == CANDIDATE) && state.isElectionNeeded) {
            LOG.info("node {} starting new election", nodeId);

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
            LOG.info("node {} sending RequestVote to peers", nodeId);

            final Object lock = new Object();

            int currentTerm = state.currentTerm;
            int lastLogTerm = state.raftLog.lastLogTerm();
            int lastLogIndex = state.raftLog.lastLogIndex();
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
        if (state.role == CANDIDATE) {
            LOG.info("node {} becoming leader", nodeId);

            int voteCount = peers.values().stream().mapToInt(peer -> peer.voteGranted ? 1 : 0).sum();
            if (voteCount + 1 > peers.size() / 2) {
                state.role = LEADER;
                state.leaderId = nodeId;
//                peers.forEach((peerId, peer) -> peer.nextIndex = state.raftLog.size() + 1);
                peers.forEach((peerId, peer) -> peer.nextIndex = state.raftLog.size());
            }
        }
    }

    private void sendAppendEntriesToPeers() {
        if (state.role == LEADER) {
            LOG.info("node {} sending AppendEntries to peers", nodeId);

            final Object lock = new Object();

            List<LogEntry> raftLog = state.raftLog.copyOfEntries();
            int logSize = raftLog.size();
            int leaderId = state.leaderId;
            int currentTerm = state.currentTerm;
            int commitIndex = state.commitIndex;
            forEachPeerParallel((peer) -> {
                if (peer.matchIndex < logSize) {
                    peer.rpcDue = System.currentTimeMillis() + ELECTION_TIMEOUT_MS / 2;    //TODO: revise
                    int prevLogIndex = peer.nextIndex - 1;
                    int prevLogTerm = state.raftLog.logTerm(prevLogIndex);
                    int lastIndex = state.raftLog.lastLogIndex();
                    List<LogEntry> entries = raftLog.subList(peer.nextIndex, lastIndex);

                    AppendEntriesRequest request = new AppendEntriesRequest(
                            currentTerm, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex)
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
//                                peer.nextIndex = Math.max(1, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                                peer.nextIndex = Math.max(0, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                            }
                        }
                    }
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (state.role == LEADER) {
//            LOG.info("node {} advancing commit index", nodeId);

            int majorityIdx = peers.size() / 2;
            int n = IntStream.concat(
                    peers.values().stream().flatMapToInt(peer -> IntStream.of(peer.matchIndex)),
                    IntStream.of(state.raftLog.size())) // append len(log)
                    .sorted()
                    .boxed().collect(Collectors.toList()).get(majorityIdx);
            if (state.raftLog.logTerm(n) == state.currentTerm) {
                state.commitIndex = n;
            }
        }
    }

    private void advanceStateMachine() {
        if (state.role == LEADER) {
//            LOG.info("node {} advancing state machine", nodeId);

            while (state.lastApplied < state.commitIndex) {
                state.lastApplied++;
                // apply on StateMachine
            }
        }
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
        if (state.currentTerm == request.getTerm() && (state.votedFor == -1 || state.votedFor == request.getCandidateId()) &&
                (request.getLastLogTerm() > state.raftLog.lastLogTerm()
                        || (request.getLastLogTerm() == state.raftLog.lastLogTerm() && request.getLastLogIndex() >= state.raftLog.size()))
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
                            state.raftLog.logTerm(request.getPrevLogIndex()) == request.getPrevLogTerm());
            int index;
            if (success) {
                index = request.getPrevLogIndex();
                for (int i = 0; i < request.getEntries().size(); i++) {
                    index++;
                    if (state.raftLog.logTerm(index) != request.getEntries().get(i).getTerm()) {
                        while (state.raftLog.size() > index) {
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
        if (state.leaderId == -1) {
            throw new IllegalStateException("leader unresolved");
        }
        if (state.role == LEADER) {
            state.raftLog.push(new LogEntry(request.getCommand(), state.currentTerm));
            int entryIndex = state.raftLog.lastLogIndex();
            update();
            boolean isApplied = state.raftLog.logTerm(entryIndex) == state.currentTerm;
            return new StateMachineResponse(isApplied).responseTo(request);
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
/*            matchIndex = 0;
            nextIndex = 1;*/
            matchIndex = -1;
            nextIndex = 0;
        }
    }

}
