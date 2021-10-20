package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.mboysan.consensus.RaftState.Role.CANDIDATE;
import static com.mboysan.consensus.RaftState.Role.FOLLOWER;
import static com.mboysan.consensus.RaftState.Role.LEADER;

public class RaftNode extends AbstractNode<RaftPeer> implements RaftRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    private final Lock updateLock = new ReentrantLock();

    private final long updateIntervalMs;
    private long electionTimeoutMs;
    private long nextElectionTime;

    private final RaftState state = new RaftState();
    private Consumer<String> stateMachine = null;

    public RaftNode(RaftConfig config, Transport transport) {
        super(config, transport);

        this.electionTimeoutMs = config.electionTimeoutMs();
        this.updateIntervalMs = config.updateIntervalMs();
    }

    @Override
    RaftRPC getRPC() {
        return new RaftClient(getTransport());
    }

    @Override
    RaftPeer createPeer(int peerId) {
        // we do not add ourself as a peer
        return peerId != getNodeId() ? new RaftPeer(peerId) : null;
    }

    synchronized void registerStateMachine(Consumer<String> stateMachine) {
        Objects.requireNonNull(stateMachine);
        if (this.stateMachine != null) {
            throw new IllegalArgumentException("stateMachine handler already registered");
        }
        this.stateMachine = stateMachine;
    }

    @Override
    Future<Void> startNode() {
        int electId = (getNodeId() % (peers.size() + 1)) + 1;
        this.electionTimeoutMs = electionTimeoutMs * electId;
        LOGGER.info("node-{} modified electionTimeoutMs={}", getNodeId(), electionTimeoutMs);

        nextElectionTime = getTimers().currentTime() + electionTimeoutMs;
        getTimers().schedule("updateTimer-node" + getNodeId(), this::tryUpdate, updateIntervalMs, updateIntervalMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                synchronized (this) {
                    if (state.leaderId != -1) {
                        return null;
                    }
                }
                getTimers().sleep(updateIntervalMs);
            }
        });
    }

    @Override
    void shutdownNode() {
        // no special logic needed
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    private void tryUpdate() {
        if (updateLock.tryLock()) {
            try {
                LOGGER.debug("node-{} update timeout, time={}", getNodeId(), getTimers().currentTime());
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
            LOGGER.info("node-{} starting new election", getNodeId());

            state.currentTerm++;
            state.votedFor = getNodeId();
            state.role = CANDIDATE;

            // reset all state for peers
            peers.forEach((i, peer) -> peer.reset());
        }
    }

    private boolean isElectionNeeded() {
        long currentTime = getTimers().currentTime();
        if (currentTime >= nextElectionTime) {
            nextElectionTime = currentTime + electionTimeoutMs;
            boolean isElectionNeeded = state.role != LEADER && !state.seenLeader;
            state.seenLeader = false;
            if (isElectionNeeded) {
                LOGGER.info("node-{} needs a new election", getNodeId());
            }
            return isElectionNeeded;
        }
        return false;
    }

    private void sendRequestVoteToPeers() {
        if (state.role == CANDIDATE) {
            LOGGER.info("node-{} sending RequestVote to peers", getNodeId());

            final Object lock = new Object();

            int currentTerm = state.currentTerm;
            int lastLogTerm = state.raftLog.lastLogTerm();
            int lastLogIndex = state.raftLog.lastLogIndex();
            forEachPeerParallel(peer -> {
                RequestVoteRequest request = new RequestVoteRequest(currentTerm, getNodeId(), lastLogIndex, lastLogTerm)
                        .setSenderId(getNodeId())
                        .setReceiverId(peer.peerId);
                try {
                    RequestVoteResponse response = getRPC().requestVote(request);

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
                state.leaderId = getNodeId();
                state.seenLeader = true;
                peers.forEach((peerId, peer) -> peer.nextIndex = state.raftLog.size());
                LOGGER.info("node-{} thinks it's leader", getNodeId());
            }
        }
    }

    private void sendAppendEntriesToPeers() {
        if (state.role == LEADER) {
            LOGGER.info("node-{} sending AppendEntries to peers", getNodeId());

            final Object lock = new Object();

            int leaderId = state.leaderId;
            int currentTerm = state.currentTerm;
            int commitIndex = state.commitIndex;
            forEachPeerParallel(peer -> {
                if (peer.matchIndex < state.raftLog.size()) {
                    int prevLogIndex = peer.nextIndex - 1;
                    int prevLogTerm = state.raftLog.logTerm(prevLogIndex);
                    List<LogEntry> entries = state.raftLog.getEntriesFrom(peer.nextIndex);

                    AppendEntriesRequest request = new AppendEntriesRequest(
                            currentTerm, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex)
                            .setSenderId(getNodeId())
                            .setReceiverId(peer.peerId);
                    try {
                        AppendEntriesResponse response = getRPC().appendEntries(request);

                        synchronized (lock) {
                            if (state.currentTerm < response.getTerm()) {
                                stepDown(response.getTerm());
                            } else if (state.role == LEADER && state.currentTerm == response.getTerm()) {
                                if (response.isSuccess()) {
                                    peer.matchIndex = response.getMatchIndex();
                                    peer.nextIndex = response.getMatchIndex() + 1;
                                } else {
                                    peer.nextIndex = Math.max(0, peer.nextIndex - 1);   // retries with decremented index
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
                for (RaftPeer peer : peers.values()) {
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
        boolean isAdvanced = false;
        while (state.lastApplied < state.commitIndex) {
            isAdvanced = true;
            state.lastApplied++;
            if (stateMachine != null) {
                stateMachine.accept(state.raftLog.get(state.lastApplied).getCommand());
            }
            // apply on StateMachine
        }
        if (isAdvanced) {
            synchronized (this) {
                notifyAll();
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

                advanceStateMachine();  // try syncing before leader update. TODO: check if this is okay.
                return new AppendEntriesResponse(state.currentTerm, true, state.raftLog.lastLogIndex()).responseTo(request);
            } else {
                return new AppendEntriesResponse(state.currentTerm, false).responseTo(request);
            }
        }
    }

    @Override
    public StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException {
        int leaderId;
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
                    } catch (InterruptedException e) {
                        LOGGER.warn("The request has been interrupted/cancelled for index={}", entryIndex);
                        Thread.currentThread().interrupt();
                    }
                }
                return new StateMachineResponse(isEntryApplied(entryIndex, term)).responseTo(request);
            }
            leaderId = state.leaderId;
        }

        return getRPC().stateMachineRequest(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    private boolean isEntryApplied(int entryIndex, int term) {
        return state.raftLog.logTerm(entryIndex) == term && state.lastApplied >= entryIndex;
    }

    public Future<Boolean> append(String command) {
        validateAction();
        return commandExecutor.submit(() -> {
            try {
                return stateMachineRequest(new StateMachineRequest(command)).isApplied();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return false;
        });
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private void stepDown(int newTerm) {
        LOGGER.info("node-{} stepped down at term={} because of newTerm={}", getNodeId(), state.currentTerm, newTerm);
        state.currentTerm = newTerm;
        state.role = FOLLOWER;
        state.votedFor = -1;
        state.seenLeader = false;
    }

    /*----------------------------------------------------------------------------------
     * For testing
     * ----------------------------------------------------------------------------------*/

    RaftState getState() {
        return state;
    }
}
