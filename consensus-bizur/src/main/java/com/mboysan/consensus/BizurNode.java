package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BizurNode extends AbstractNode<BizurPeer> implements BizurRPC {

    static final int DEFAULT_NUM_BUCKETS = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurNode.class);

    private final Lock updateLock = new ReentrantLock();

    private static final long UPDATE_INTERVAL_MS = 500;
    private static final long ELECTION_TIMEOUT_MS = UPDATE_INTERVAL_MS * 10;  //5000
    long electionTimeoutMs;
    private long electionTime;

    private final int numBuckets;
    private final Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>();

    private final BizurState state = new BizurState();

    BizurNode(int nodeId, Transport transport) {
        this(nodeId, transport, DEFAULT_NUM_BUCKETS);
    }

    BizurNode(int nodeId, Transport transport, int numBuckets) {
        super(nodeId, transport);
        this.numBuckets = numBuckets;
    }

    @Override
    BizurPeer createPeer(int peerId) {
        return new BizurPeer(peerId);
    }

    @Override
    Future<Void> startNode() {
        int electId = (getNodeId() % (peers.size() + 1)) + 1;
        electionTimeoutMs = ELECTION_TIMEOUT_MS * electId;
        LOGGER.info("node-{} electionTimeoutMs={}", getNodeId(), electionTimeoutMs);
        electionTime = getTimers().currentTime() + electionTimeoutMs;
        long updateTimeoutMs = UPDATE_INTERVAL_MS;
        getTimers().schedule("updateTimer-node" + getNodeId(), this::tryUpdate, updateTimeoutMs, updateTimeoutMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                synchronized (state) {
                    if (state.getLeaderId() != -1) {
                        return null;
                    }
                }
                getTimers().sleep(updateTimeoutMs);
            }
        });
    }

    @Override
    void shutdownNode() {
        // no special logic needed
    }

    /*----------------------------------------------------------------------------------
     * Algorithms
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
        int leaderId;
        synchronized (state) {
            leaderId = state.getLeaderId();
        }
        if (leaderId == -1) {   // if no leader, try to become leader
            getTimers().sleep(electionTimeoutMs);
            startElection();
            update();
            return;
        }
        if (leaderId == getNodeId()) {
            return; // I am the leader, no need to take action
        }

        HeartbeatRequest request = new HeartbeatRequest(System.currentTimeMillis())
                .setSenderId(getNodeId())
                .setReceiverId(leaderId);
        try {
            HeartbeatResponse response = getRPC(getTransport()).heartbeat(request);
            if (LOGGER.isDebugEnabled()) {
                long elapsed = response.getSendTimeMs() - request.getSendTimeMs();
                LOGGER.debug("peer-{} heartbeat elapsed={}", leaderId, elapsed);
            }
        } catch (IOException e) {
            LOGGER.error("peer-{} IO exception for request={}, cause={}", leaderId, request, e.getMessage());
            // leader is dead, lets start the election.
            getTimers().sleep(electionTimeoutMs);
            startElection();
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 1: Leader Election
     * ----------------------------------------------------------------------------------*/

    private void startElection() {
        LOGGER.info("node-{} starting new election", getNodeId());
        int electId;
        synchronized (state) {
            electId = state.incrementAndGetElectId();
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            PleaseVoteRequest request = new PleaseVoteRequest(electId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                PleaseVoteResponse response = getRPC(getTransport()).pleaseVote(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
            }
        });
        synchronized (state) {
            if (isMajorityAcked(ackCount.get())) {
                state.setLeaderId(getNodeId());
                LOGGER.info("node-{} thinks it's leader", getNodeId());
            } else {
                state.setLeaderId(-1);
            }
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 2: Bucket Replication: Write
     * ----------------------------------------------------------------------------------*/

    private boolean write(Bucket lockedBucket) throws BizurException {
        int electId;
        synchronized (state) {
            validateLeader(state.getLeaderId());
            electId = state.getElectId();
        }
        lockedBucket.setVerElectId(electId);
        lockedBucket.incrementVerCounter();

        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaWriteRequest request = new ReplicaWriteRequest(lockedBucket.getIndex(), lockedBucket.createView())
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaWriteResponse response = getRPC(getTransport()).replicaWrite(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
            }
        });
        if (isMajorityAcked(ackCount.get())) {
            return true;
        } else {
            synchronized (state) {
                state.setLeaderId(-1);  // step down
            }
            return false;
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 3: Bucket Replication: Read
     * ----------------------------------------------------------------------------------*/

    private Bucket read(int index) throws BizurException {
        int electId;
        synchronized (state) {
            validateLeader(state.getLeaderId());
            electId = state.getElectId();
        }

        if (!ensureRecovery(index, electId)) {
            LOGGER.error("node-{} could not ensure recovery of bucket index={}, electId={}", getNodeId(), index, electId);
            return null;
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC(getTransport()).replicaRead(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
            }
        });
        if (!isMajorityAcked(ackCount.get())) {
            synchronized (state) {
                state.setLeaderId(-1);   // step down
            }
            return null;
        }
        return getBucket(index);
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 4: Bucket Replication: Recovery
     * ----------------------------------------------------------------------------------*/

    private boolean ensureRecovery(int index, int electId) throws BizurException {
        Bucket bucket = getBucket(index).lock();
        try {
            if (electId == bucket.getVerElectId()) {
                return true;
            }
        } finally {
            bucket.unlock();
        }
        AtomicReference<BucketView> maxVerBucketView = new AtomicReference<>(null);
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC(getTransport()).replicaRead(request);
                if (response.isAcked()) {
                    BucketView bucketView = response.getBucketView();
                    synchronized (maxVerBucketView) {
                        if (!maxVerBucketView.compareAndSet(null, bucketView)) {
                            if (bucketView.compareTo(maxVerBucketView.get()) > 0) {
                                maxVerBucketView.set(bucketView);
                            }
                        }
                    }
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
            }
        });
        if (isMajorityAcked(ackCount.get())) {
            BucketView bucketView = maxVerBucketView.get();
            bucket.lock();
            try {
                bucket.setVerElectId(electId)
                        .setVerCounter(0)
                        .setBucketMap(bucketView.getBucketMap());
                return write(bucket);
            } finally {
                bucket.unlock();
            }
        } else {
            synchronized (state) {
                state.setLeaderId(-1);
            }
            return false;
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 5: Key-Value API
     * ----------------------------------------------------------------------------------*/

    private String apiGet(String key) throws BizurException {
        Objects.requireNonNull(key);

        int index = hashKey(key);
        Bucket bucket = read(index);
        if (bucket != null) {
            bucket.lock();
            try {
                return bucket.getOp(key);
            } finally {
                bucket.unlock();
            }
        }
        throw new BizurException(String.format("get failed on node-%d for key=%s", getNodeId(), key));
    }

    private boolean apiSet(String key, String value) throws BizurException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        int index = hashKey(key);
        Bucket bucket = read(index);
        if (bucket != null) {
            bucket.lock();
            try {
                bucket.putOp(key, value);
                return write(bucket); // TODO: investigate if we need to revert if write fails
            } finally {
                bucket.unlock();
            }
        }
        throw new BizurException(String.format("set failed on node-%d for key=%s", getNodeId(), key));
    }

    /*----------------------------------------------------------------------------------
     * Internal RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest request) {
        return new HeartbeatResponse(System.currentTimeMillis()).responseTo(request);
    }

    @Override
    public PleaseVoteResponse pleaseVote(PleaseVoteRequest request) {
        synchronized (state) {
            if (request.getElectId() > state.getVotedElectId()) {
                state.setVotedElectId(request.getElectId());
                state.setLeaderId(request.getSenderId()); // "update" vote
                return new PleaseVoteResponse(true).responseTo(request);
            } else if (request.getElectId() == state.getVotedElectId() && request.getSenderId() == state.getLeaderId()) {
                return new PleaseVoteResponse(true).responseTo(request);
            }
            return new PleaseVoteResponse(false).responseTo(request);
        }
    }

    @Override
    public ReplicaReadResponse replicaRead(ReplicaReadRequest request) {
        int index = request.getBucketIndex();
        int electId = request.getElectId();
        int source = request.getSenderId();
        synchronized (state) {
            Bucket bucket = getBucket(index).lock();
            try {
                if (electId < state.getVotedElectId()) {
                    return new ReplicaReadResponse(false, null).responseTo(request);
                } else {
                    state.setVotedElectId(electId);
                    state.setLeaderId(source);  // "update" vote
                    return new ReplicaReadResponse(true, bucket.createView()).responseTo(request);
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) {
        BucketView bucketView = request.getBucketView();
        synchronized (state) {
            Bucket bucket = getBucket(request.getBucketIndex()).lock();
            try {
                if (bucketView.getVerElectId() < state.getVotedElectId()) {
                    return new ReplicaWriteResponse(false);
                } else {
                    state.setVotedElectId(bucketView.getVerElectId());
                    state.setLeaderId(request.getSenderId());    // "update" vote
                    bucket.setBucketMap(bucketView.getBucketMap());
                    return new ReplicaWriteResponse(true);
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    /*----------------------------------------------------------------------------------
     * Public RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public KVGetResponse get(KVGetRequest request) throws IOException {
        validateAction();
        int leaderId;
        synchronized (state) {
            leaderId = state.getLeaderId();
        }
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                String value = apiGet(request.getKey());
                return new KVGetResponse(true, null, value).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVGetResponse(false, e, null).responseTo(request);
            }
        }
        // route to leader
        return getRPC(getTransport()).get(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        validateAction();
        int leaderId;
        synchronized (state) {
            leaderId = state.getLeaderId();
        }
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                boolean success = apiSet(request.getKey(), request.getValue());
                return new KVSetResponse(success, null).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVSetResponse(false, e).responseTo(request);
            }
        }
        // route to leader
        return getRPC(getTransport()).set(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    public Future<String> get(String key) {
        return exec(() -> {
            KVGetRequest request = new KVGetRequest(key);
            KVGetResponse response = get(request);
            validateResponse(response, request);
            return response.getValue();
        });
    }

    public Future<Void> set(String key, String value) {
        return exec(() -> {
            KVSetRequest request = new KVSetRequest(key, value);
            KVSetResponse response = set(request);
            validateResponse(response, request);
            return null;
        });
    }

    private void validateResponse(KVOperationResponse response, Message forRequest) throws BizurException {
        if (!response.isSuccess()) {
            throw new BizurException(String.format("on node-%d: failed response=[%s] for request=[%s]",
                    getNodeId(), response, forRequest.toString()));
        }
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private void validateLeader(int leaderId) throws BizurException {
        if (leaderId != getNodeId()) {
            throw new BizurException(String.format("node-%d is not the leader", getNodeId()));
        }
    }

    private Bucket getBucket(int index) {
        return bucketMap.computeIfAbsent(index, Bucket::new);
    }

    private boolean isMajorityAcked(int voteCount) {
        return voteCount > peers.size() / 2;
    }

    private int hashKey(String key) {
        return key.hashCode() % numBuckets;
    }

    private void logErrorForRequest(Exception exception, Message request) {
        LOGGER.error("err on node-{}: exception={}, request={}", getNodeId(), exception.getMessage(), request.toString());
    }

    private <T> Future<T> exec(CheckedSupplier<T> supplier) {
        validateAction();
        return commandExecutor.submit(() -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                throw e;
            }
        });
    }

    /*----------------------------------------------------------------------------------
     * For testing
     * ----------------------------------------------------------------------------------*/

    BizurState getState() {
        return state;
    }

    int getNumBuckets() {
        return numBuckets;
    }

    Map<Integer, Bucket> getBucketMap() {
        return bucketMap;
    }
}
