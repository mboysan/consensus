package com.mboysan.consensus;

import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.message.AppendEntriesRequest;
import com.mboysan.consensus.message.AppendEntriesResponse;
import com.mboysan.consensus.message.CheckRaftIntegrityRequest;
import com.mboysan.consensus.message.CheckRaftIntegrityResponse;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.LogEntry;
import com.mboysan.consensus.message.RequestVoteRequest;
import com.mboysan.consensus.message.RequestVoteResponse;
import com.mboysan.consensus.message.StateMachineRequest;
import com.mboysan.consensus.message.StateMachineResponse;
import com.mboysan.consensus.util.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.mboysan.consensus.RaftState.Role.CANDIDATE;
import static com.mboysan.consensus.RaftState.Role.FOLLOWER;
import static com.mboysan.consensus.RaftState.Role.LEADER;

public class RaftNode extends AbstractNode<RaftPeer> implements RaftRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    private final RaftClient rpcClient;

    private boolean notified = false;

    private final boolean isStronglyConsistent;
    private final long updateIntervalMs;
    private long electionTimeoutMs;
    private long nextElectionTime;

    private final RaftState state = new RaftState();
    private Consumer<String> stateMachine = null;

    public RaftNode(RaftConfig config, Transport transport) {
        super(config, transport);
        this.rpcClient = new RaftClient(transport);

        this.isStronglyConsistent = "strong".equals(config.consistency());
        this.updateIntervalMs = config.updateIntervalMs();
        this.electionTimeoutMs = config.electionTimeoutMs();
    }

    @Override
    RaftClient rpc() {
        return rpcClient;
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
        this.nextElectionTime = getScheduler().currentTime() + electionTimeoutMs;
        LOGGER.info("node-{} modified electionTimeoutMs={}, nextElectionTime={}",
                getNodeId(), electionTimeoutMs, nextElectionTime);
        getScheduler().schedule("updateTimer-node" + getNodeId(), this::update, updateIntervalMs, updateIntervalMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                synchronized (this) {
                    if (state.leaderId != -1) {
                        return null;
                    }
                }
                getScheduler().sleep(updateIntervalMs);
            }
        });
    }

    @Override
    void shutdownNode() {
        synchronized (this) {
            state.reset();
        }
    }

    /*----------------------------------------------------------------------------------
     * Rules for Servers
     * ----------------------------------------------------------------------------------*/

    @Override
    synchronized void update() {
        LOGGER.debug("node-{} update timeout, time={}", getNodeId(), getScheduler().currentTime());

        LOGGER.trace("node-{} previous state [{}]", getNodeId(), state);

        startNewElection();
        sendRequestVoteToPeers();
        becomeLeader();
        sendAppendEntriesToPeers();
        advanceCommitIndex();
        advanceStateMachine();

        LOGGER.trace("node-{} current state [{}]", getNodeId(), state);
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
        long currentTime = getScheduler().currentTime();
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
                    RequestVoteResponse response = rpc().requestVote(request);

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
                peers.forEach((peerId, peer) -> peer.nextIndex = (state.raftLog.lastLogIndex() + 1));
                LOGGER.info("node-{} thinks it's leader", getNodeId());
            }
        }
    }

    private void sendAppendEntriesToPeers() {
        if (state.role == LEADER) {
            LOGGER.debug("node-{} sending AppendEntries to peers", getNodeId());

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
                        AppendEntriesResponse response = rpc().appendEntries(request);

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
                        peer.reset();   // reset state for the peer as we think it is dead.
                    }
                }
            });
        }
    }

    private void advanceCommitIndex() {
        if (state.role == LEADER) {
            /* If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
               and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).*/
            int n = IntStream.concat(
                            peers.values().stream().flatMapToInt(peer -> IntStream.of(peer.matchIndex)),
                            IntStream.of(state.raftLog.lastLogIndex())) // append our view of the matchIndex
                    .max().orElseThrow();
            if (n > state.commitIndex) {
                int countGreaterEquals = state.raftLog.lastLogIndex() >= n ? 1 : 0;
                for (RaftPeer peer : peers.values()) {
                    if (peer.matchIndex >= n) {
                        countGreaterEquals++;
                    }
                }
                if (countGreaterEquals > peers.size() / 2
                        && state.raftLog.logTerm(n) == state.currentTerm) {
                    state.commitIndex = n;
                }
            }
        }
    }

    private void advanceStateMachine() {
        while (state.lastApplied < state.commitIndex) {
            state.lastApplied++;
            if (stateMachine != null) {
                // apply on StateMachine
                stateMachine.accept(state.raftLog.get(state.lastApplied).command());
            }
        }
        doNotifyAll();
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
            if (state.leaderId != request.getLeaderId()) {
                LOGGER.info("node-{} claims to be the LEADER of term {}", request.getLeaderId(), state.currentTerm);
            }
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

                // this is needed to apply the changes to the state machine
                update();

                return new AppendEntriesResponse(state.currentTerm, true, state.raftLog.lastLogIndex());
            } else {
                return new AppendEntriesResponse(state.currentTerm, false);
            }
        }
    }

    @Override
    public StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException {
        validateAction();
        int leaderId;
        synchronized (this) {
            if (state.leaderId == -1) {
                throw new IllegalStateException("leader unresolved");
            }
            if (state.role == LEADER) {
                state.raftLog.push(new LogEntry(request.getCommand(), state.currentTerm));
                int entryIndex = state.raftLog.lastLogIndex();
                if (isStronglyConsistent) {
                    update();   // sync for strong consistency
                }
                int term = state.currentTerm;
                if (!isEntryApplied(entryIndex, term)) { // if not applied
                    try {
                        doWait(); // wait for state machine advancement.
                    } catch (InterruptedException e) {
                        LOGGER.warn("The request has been interrupted/cancelled for index={}", entryIndex);
                        Thread.currentThread().interrupt();
                    }
                }
                return new StateMachineResponse(isEntryApplied(entryIndex, term));
            }
            leaderId = state.leaderId;
        }

        return routeMessage(request, leaderId);
    }

    private boolean isEntryApplied(int entryIndex, int term) {
        return state.raftLog.logTerm(entryIndex) == term && state.lastApplied >= entryIndex;
    }

    @Override
    public CheckRaftIntegrityResponse checkRaftIntegrity(CheckRaftIntegrityRequest request) throws IOException {
        validateAction();
        if (request.isRoutingNeeded()) {
            return routeMessage(request);
        }
        synchronized (this) {
            switch (request.getLevel()) {
                case CoreConstants.StateLevels.INFO_STATE -> {
                    return new CheckRaftIntegrityResponse(true, state.getIntegrityHash(), state.toInfoString());
                }
                case CoreConstants.StateLevels.DEBUG_STATE -> {
                    return new CheckRaftIntegrityResponse(true, state.getIntegrityHash(), state.toDebugString());
                }
                case CoreConstants.StateLevels.INFO_STATE_FROM_ALL,
                     CoreConstants.StateLevels.DEBUG_STATE_FROM_ALL -> {
                    int requestLevel = request.getLevel();
                    final int levelOverride = requestLevel == CoreConstants.StateLevels.INFO_STATE_FROM_ALL
                            ? CoreConstants.StateLevels.INFO_STATE
                            : CoreConstants.StateLevels.DEBUG_STATE;

                    CheckRaftIntegrityResponse thisNodeResponse = this.checkRaftIntegrity(
                            new CheckRaftIntegrityRequest(levelOverride));

                    String thisNodeIntegrityHash = thisNodeResponse.getIntegrityHash();
                    String thisNodeState = thisNodeResponse.getState();

                    Map<Integer, String> integrityHashes = new ConcurrentHashMap<>();
                    Map<Integer, String> states = new ConcurrentHashMap<>();

                    integrityHashes.put(getNodeId(), thisNodeIntegrityHash);
                    states.put(getNodeId(), thisNodeState);

                    forEachPeerParallel(peer -> {
                        CheckRaftIntegrityRequest raftRequest = new CheckRaftIntegrityRequest(levelOverride)
                                .setSenderId(getNodeId())
                                .setReceiverId(peer.peerId);
                        try {
                            CheckRaftIntegrityResponse raftResponse = rpc().checkRaftIntegrity(raftRequest);
                            integrityHashes.put(peer.peerId, raftResponse.getIntegrityHash());
                            states.put(peer.peerId, raftResponse.getState());
                        } catch (IOException e) {
                            LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, raftRequest, e.getMessage());
                        }
                    });

                    int majorityCount = (peers.size() / 2) + 1;
                    Optional<String> majorityHash = HashUtil.findCommonHash(integrityHashes.values(), majorityCount);
                    String leaderHash = integrityHashes.get(state.leaderId);

                    LOGGER.info("node-{} integrityHash={}, majorityHash={}, leaderHash={}",
                            getNodeId(), thisNodeIntegrityHash, majorityHash.orElse(null), leaderHash);

                    if (majorityHash.isPresent()) {
                        if (!majorityHash.get().equals(leaderHash)) {
                            LOGGER.warn("leader integrity hash={} is different than majorityHash={}",
                                    leaderHash, majorityHash.get());
                        }
                        return new CheckRaftIntegrityResponse(true, majorityHash.get(), states.toString());
                    }

                    return new CheckRaftIntegrityResponse(false, thisNodeIntegrityHash, states.toString());
                }
                default -> throw new IOException("unsupported level=" + request.getLevel());
            }
        }
    }

    @Override
    public CustomResponse customRequest(CustomRequest request) throws IOException {
        validateAction();
        if (request.isRoutingNeeded()) {
            return routeMessage(request);
        }
        if (CustomRequest.Command.PING.equals(request.getCommand())) {
            return new CustomResponse(true, null, CustomResponse.CommonPayload.PONG);
        }
        return new CustomResponse(
                false, new UnsupportedOperationException(request.getCommand()), null);
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

    private synchronized void doWait() throws InterruptedException {
        while (!notified) {
            wait();
        }
        notified = false;
    }

    private synchronized void doNotifyAll() {
        notified = true;
        notifyAll();
    }

    /*----------------------------------------------------------------------------------
     * For testing
     * ----------------------------------------------------------------------------------*/

    RaftState getState() {
        return state;
    }
}
