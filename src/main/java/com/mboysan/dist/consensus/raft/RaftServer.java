package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;
import com.mboysan.util.TimerQueue;
import com.mboysan.util.Timers;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.mboysan.dist.consensus.raft.State.Role.*;

public class RaftServer implements RaftRPC {

    private final Logger LOGGER = LoggerFactory.getLogger(RaftServer.class);

    private final ExecutorService peerExecutor;
    private final ExecutorService commandExecutor;
    private final Lock updateLock = new ReentrantLock();

    private static final long UPDATE_INTERVAL_MS = 500;
    private static final long ELECTION_TIMEOUT_MS = UPDATE_INTERVAL_MS * 20;  //10000
    long electionTimeoutMs;
    private long electionTime;
    private final Timers timers;

    private volatile boolean isRunning;

    private final Transport transport;
    private final int nodeId;

    final State state = new State();
    final Map<Integer, Peer> peers = new HashMap<>();

    public RaftServer(int nodeId, Transport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        transport.addServer(nodeId, this);
        timers = createTimers();
        peerExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new BasicThreadFactory.Builder().namingPattern("RaftPeerExec-" + nodeId + "-%d").daemon(true).build()
        );
        commandExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new BasicThreadFactory.Builder().namingPattern("RaftCmdExec-" + nodeId + "-%d").daemon(true).build()
        );
    }

    Random createRandom(long seed) {
        System.out.println("node-" + nodeId + ", SEED=" + seed);
        LOGGER.error("node-" + nodeId + ", SEED=" + seed);
        return new Random(seed);
    }

    Timers createTimers() {
        return new TimerQueue();
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public synchronized void onServerListChanged(Set<Integer> serverIds) {
        // first, we add new peers for each new serverId. (we do not add ourself as a peer)
        serverIds.forEach(nodeId -> {
            peers.computeIfAbsent(nodeId, id -> id != this.nodeId ? new Peer(nodeId) : null);
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
                futures.add(peerExecutor.submit(() -> peerConsumer.accept(peer)));
            } else {
                throw new UnsupportedOperationException();
                // we don't send the request to self.
//                peerConsumer.accept(peer);
            }
        });
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error(e.getMessage(), e);
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public synchronized Future<Void> start() {
        isRunning = true;

        Random random = createRandom(System.currentTimeMillis());
        int max = (int) (ELECTION_TIMEOUT_MS / 1000);
        int min = (int) ((UPDATE_INTERVAL_MS * 10) / 1000);

        electionTimeoutMs = (random.nextInt((max - min) + 1) + min) * 1000;
        LOGGER.info("node-{} electionTimeoutMs={}", nodeId, electionTimeoutMs);
        electionTime = timers.currentTime() + electionTimeoutMs;
        long updateTimeoutMs = UPDATE_INTERVAL_MS;
        timers.schedule("updateTimer-node" + nodeId, this::onUpdateTimeout, updateTimeoutMs, updateTimeoutMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                synchronized (this) {
                    if (state.leaderId != -1) {
                        return null;
                    }
                }
                timers.sleep(electionTimeoutMs);
            }
        });
    }

    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        timers.shutdown();
        commandExecutor.shutdown();
        peerExecutor.shutdown();
        transport.removeServer(nodeId);
        peers.clear();
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    private void onUpdateTimeout() {
        if (updateLock.tryLock()) {
            try {
                LOGGER.debug("node-{} update timeout, time={}", nodeId, timers.currentTime());
                update();
            } finally {
                updateLock.unlock();
            }
        } else {
            LOGGER.debug("update in progress, skipped.");
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

    private void startNewElection() {
        if ((state.role == FOLLOWER || state.role == CANDIDATE) && isElectionNeeded()) {
            LOGGER.info("node-{} starting new election", nodeId);

            state.currentTerm++;
            state.votedFor = nodeId;
            state.role = CANDIDATE;

            // reset all state for peers
            peers.forEach((i, peer) -> peer.reset());
        }
    }

    private boolean isElectionNeeded() {
        long currentTime = timers.currentTime();
        if (currentTime >= electionTime) {
            electionTime = currentTime + electionTimeoutMs;
            boolean isElectionNeeded = state.role != LEADER && !state.seenLeader;
            state.seenLeader = false;
            if (isElectionNeeded) {
                LOGGER.info("node-{} needs a new election", nodeId);
            }
            return isElectionNeeded;
        }
        return false;
    }

    private void sendRequestVoteToPeers() {
        if (state.role == CANDIDATE) {
            LOGGER.info("node-{} sending RequestVote to peers", nodeId);

            final Object lock = new Object();

            int currentTerm = state.currentTerm;
            int lastLogTerm = state.raftLog.lastLogTerm();
            int lastLogIndex = state.raftLog.lastLogIndex();
            forEachPeerParallel((peer) -> {
                RequestVoteRequest request = new RequestVoteRequest(currentTerm, nodeId, lastLogIndex, lastLogTerm)
                        .setSenderId(nodeId)
                        .setReceiverId(peer.peerId);
                try {
                    RequestVoteResponse response = getRPC(transport).requestVote(request);

                    synchronized (lock) {
                        if (state.currentTerm < response.getTerm()) {
                            stepDown(response.getTerm());
                        }
                        if (state.role == CANDIDATE && state.currentTerm == response.getTerm()) {
                            peer.voteGranted = response.isVoteGranted();
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
                }
            });
        }
    }

    private void becomeLeader() {
        if (state.role == CANDIDATE) {
            int voteCount = peers.values().stream().mapToInt(peer -> peer.voteGranted ? 1 : 0).sum();
            if (voteCount + 1 > peers.size() / 2) {
                state.role = LEADER;
                state.leaderId = nodeId;
                state.seenLeader = true;
                peers.forEach((peerId, peer) -> peer.nextIndex = state.raftLog.size());
                LOGGER.info("node-{} thinks it's leader", nodeId);
            }
        }
    }

    private void sendAppendEntriesToPeers() {
        if (state.role == LEADER) {
            LOGGER.info("node-{} sending AppendEntries to peers", nodeId);

            final Object lock = new Object();

            int leaderId = state.leaderId;
            int currentTerm = state.currentTerm;
            int commitIndex = state.commitIndex;
            forEachPeerParallel((peer) -> {
                if (peer.matchIndex < state.raftLog.size()) {
                    int prevLogIndex = peer.nextIndex - 1;
                    int prevLogTerm = state.raftLog.logTerm(prevLogIndex);
                    List<LogEntry> entries = state.raftLog.getEntriesFrom(peer.nextIndex);

                    AppendEntriesRequest request = new AppendEntriesRequest(
                            currentTerm, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex)
                            .setSenderId(nodeId)
                            .setReceiverId(peer.peerId);
                    try {
                        AppendEntriesResponse response = getRPC(transport).appendEntries(request);

                        synchronized (lock) {
                            if (state.currentTerm < response.getTerm()) {
                                stepDown(response.getTerm());
                            } else if (state.role == LEADER && state.currentTerm == response.getTerm()) {
                                if (response.isSuccess()) {
                                    peer.matchIndex = response.getMatchIndex();
                                    peer.nextIndex = response.getMatchIndex() + 1;
                                } else {
                                    peer.nextIndex = Math.max(0, peer.nextIndex - 1);   // decrement peer.nextIndex, TODO: retry
                                }
                            }
                        }
                    } catch (IOException e) {
                        LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
                    }
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (state.role == LEADER) {
//            LOG.info("node-{} advancing commit index", nodeId);

/*            int majorityIdx = peers.size() / 2;
            int n = IntStream.concat(
                    peers.values().stream().flatMapToInt(peer -> IntStream.of(peer.matchIndex)),
                    IntStream.of(state.raftLog.size())) // append len(log)
                    .sorted()
                    .boxed().collect(Collectors.toList()).get(majorityIdx);
            if (state.raftLog.logTerm(n) == state.currentTerm) {
                state.commitIndex = n;
            }*/

            /* If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
               and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).*/
            int N = IntStream.concat(
                    peers.values().stream().flatMapToInt(peer -> IntStream.of(peer.matchIndex)),
                    IntStream.of(state.raftLog.lastLogIndex())) // append our view of the matchIndex
                    .max().orElseThrow();
            if (N > state.commitIndex) {
                int countGreaterEquals = state.raftLog.lastLogIndex() >= N ? 1 : 0;
                for (Peer peer : peers.values()) {
                    if (peer.matchIndex >= N) {
                        countGreaterEquals++;
                    }
                }
                if (countGreaterEquals > peers.size() / 2
                        && state.raftLog.logTerm(N) == state.currentTerm) {
                    state.commitIndex = N;
                }
            }
        }
    }

    private void advanceStateMachine() {
//        if (state.role == LEADER) {
//            LOG.info("node-{} advancing state machine", nodeId);

            boolean isAdvanced = false;
            while (state.lastApplied < state.commitIndex) {
                isAdvanced = true;
                state.lastApplied++;
                // apply on StateMachine
            }
            if (isAdvanced) {
                notifyAll();
            }
//        }
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
                        || (request.getLastLogTerm() == state.raftLog.lastLogTerm() && request.getLastLogIndex() >= state.raftLog.lastLogIndex()))
        ) {
            granted = true;
            state.votedFor = request.getCandidateId();
            state.seenLeader = true;
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
            state.seenLeader = true;
            boolean success = (request.getPrevLogIndex() < 0) ||
                    (request.getPrevLogIndex() <= state.raftLog.lastLogIndex() &&
                            state.raftLog.logTerm(request.getPrevLogIndex()) == request.getPrevLogTerm());
            if (success) {
                state.raftLog.removeEntriesFrom(request.getPrevLogIndex() + 1);
                for (LogEntry entry : request.getEntries()) {
                    state.raftLog.push(entry);
                }

                state.commitIndex = Math.min(request.getLeaderCommit(), state.raftLog.lastLogIndex());
                state.votedFor = request.getLeaderId();

                return new AppendEntriesResponse(state.currentTerm, true, state.raftLog.lastLogIndex()).responseTo(request);
            } else {
                return new AppendEntriesResponse(state.currentTerm, false).responseTo(request);
            }
        }
    }

    @Override
    public StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException {
        synchronized (this) {
            if (state.leaderId == -1) {
                throw new IllegalStateException("leader unresolved");
            }
            if (state.role == LEADER) {
                state.raftLog.push(new LogEntry(request.getCommand(), state.currentTerm));
                int entryIndex = state.raftLog.lastLogIndex();
                update();
                int term = state.currentTerm;
                if (!isEntryApplied(entryIndex, term)) { // if not applied
                    try {
                        wait(); // after calling append(), the future returned can be cancelled, which will throw
                                // the following exception
                    } catch (Exception e) {
                        LOGGER.warn("The request has been interrupted/cancelled for index={}", entryIndex);
                    }
                }
                return new StateMachineResponse(isEntryApplied(entryIndex, term)).responseTo(request);
            }
        }

        return getRPC(transport).stateMachineRequest(request.setReceiverId(state.leaderId).setSenderId(nodeId))
                .responseTo(request);
    }

    private boolean isEntryApplied(int entryIndex, int term) {
        return state.raftLog.logTerm(entryIndex) == term && state.lastApplied >= entryIndex;
    }

    public Future<Boolean> append(String command) {
        return commandExecutor.submit(() -> {
            try {
                return stateMachineRequest(new StateMachineRequest(command)).isApplied();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return false;
        });
    }

    synchronized void forceNotifyAll() {
        notifyAll();
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private void stepDown(int newTerm) {
        LOGGER.info("node-{} stepped down at term={} because of newTerm={}", nodeId, state.currentTerm, newTerm);
        state.currentTerm = newTerm;
        state.role = FOLLOWER;
        state.votedFor = -1;
        state.seenLeader = false;
    }

    /*----------------------------------------------------------------------------------
     * Inner Enum(s) / Class(es)
     * ----------------------------------------------------------------------------------*/

    private static class Peer {
        final int peerId;
        boolean voteGranted;
        /** for each server, index of highest log entry known to be replicated on server (initialized to 0,
         * increases monotonically) */
        int matchIndex;
        /** for each server, index of the next log entry to send to that server (initialized to leader last
         * log index, i.e. unlike Raft paper states which is last log index + 1) */
        int nextIndex;

        private Peer(int peerId) {
            this.peerId = peerId;
            reset();
        }
        void reset() {
            voteGranted = false;
            matchIndex = -1;
            nextIndex = 0;
        }
    }

}
