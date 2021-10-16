package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BizurNode extends AbstractNode<BizurPeer> implements BizurRPC {

    private static final SecureRandom RNG = new SecureRandom();

    static final int DEFAULT_NUM_BUCKETS = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurNode.class);

    private final Lock updateLock = new ReentrantLock();

    private static final long UPDATE_INTERVAL_MS = 500;
    private static final long ELECTION_TIMEOUT_MS = UPDATE_INTERVAL_MS * 10;  //5000
    long electionTimeoutMs;
    private long electionTime;

    private final int numBuckets;
    private final Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>();

    private final BizurState bizurState = new BizurState();

    public BizurNode(int nodeId, Transport transport) {
        this(nodeId, transport, DEFAULT_NUM_BUCKETS);
    }

    public BizurNode(int nodeId, Transport transport, int numBuckets) {
        super(nodeId, transport);
        this.numBuckets = numBuckets;
    }

    @Override
    BizurRPC getRPC() {
        return new BizurClient(getTransport());
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
                synchronized (bizurState) {
                    if (bizurState.getLeaderId() != -1) {
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
        synchronized (bizurState) {
            leaderId = bizurState.getLeaderId();
        }
        if (leaderId == -1) {   // if no leader, try to become leader
            getTimers().sleep(electionTimeoutMs);
            new BizurRun(this).startElection();
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
            HeartbeatResponse response = getRPC().heartbeat(request);
            if (LOGGER.isTraceEnabled()) {
                long elapsed = response.getSendTimeMs() - request.getSendTimeMs();
                LOGGER.trace("peer-{} heartbeat elapsed={}", leaderId, elapsed);
            }
        } catch (IOException e) {
            LOGGER.error("peer-{} IO exception for request={}, cause={}", leaderId, request, e.getMessage());
            // leader is dead, lets start the election.
            getTimers().sleep(electionTimeoutMs);
            new BizurRun(this).startElection();
        }
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
        synchronized (bizurState) {
            if (request.getElectId() > bizurState.getVotedElectId()) {
                bizurState.setVotedElectId(request.getElectId())
                        .setLeaderId(request.getSenderId());     // "update" vote
                return new PleaseVoteResponse(true).responseTo(request);
            } else if (request.getElectId() == bizurState.getVotedElectId() && request.getSenderId() == bizurState.getLeaderId()) {
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
        Bucket bucket = getBucket(index).lock();
        try {
            synchronized (bizurState) {
                if (electId < bizurState.getVotedElectId()) {
                    return new ReplicaReadResponse(false, null).responseTo(request);
                } else {
                    bizurState.setVotedElectId(electId)
                            .setLeaderId(source);    // "update" vote
                    return new ReplicaReadResponse(true, bucket.createView()).responseTo(request);
                }
            }
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) {
        BucketView bucketView = request.getBucketView();
        Bucket bucket = getBucket(request.getBucketIndex()).lock();
        try {
            synchronized (bizurState) {
                if (bucketView.getVerElectId() < bizurState.getVotedElectId()) {
                    return new ReplicaWriteResponse(false);
                } else {
                    bizurState.setVotedElectId(bucketView.getVerElectId())
                            .setLeaderId(request.getSenderId());     // "update" vote
                    bucket.setBucketMap(bucketView.getBucketMap());
                    return new ReplicaWriteResponse(true);
                }
            }
        } finally {
            bucket.unlock();
        }
    }

    /*----------------------------------------------------------------------------------
     * Public RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public KVGetResponse get(KVGetRequest request) throws IOException {
        validateAction();
        int leaderId = getLeaderId().orElse(getRandomPeerId());
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                String value = new BizurRun(request.getCorrelationId(), this).apiGet(request.getKey());
                return new KVGetResponse(true, null, value).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVGetResponse(false, e, null).responseTo(request);
            }
        }
        // route to leader/peer
        LOGGER.debug("routing request={}, from={} to={}", request, getNodeId(), leaderId);
        return getRPC().get(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        validateAction();
        int leaderId = getLeaderId().orElse(getRandomPeerId());
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                new BizurRun(request.getCorrelationId(), this).apiSet(request.getKey(), request.getValue());
                return new KVSetResponse(true, null).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVSetResponse(false, e).responseTo(request);
            }
        }
        // route to leader/peer
        LOGGER.debug("routing request={}, from={} to={}", request, getNodeId(), leaderId);
        return getRPC().set(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) throws IOException {
        validateAction();
        int leaderId = getLeaderId().orElse(getRandomPeerId());
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                new BizurRun(request.getCorrelationId(), this).apiDelete(request.getKey());
                return new KVDeleteResponse(true, null).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVDeleteResponse(false, e).responseTo(request);
            }
        }
        // route to leader/peer
        LOGGER.debug("routing request={}, from={} to={}", request, getNodeId(), leaderId);
        return getRPC().delete(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException {
        validateAction();
        int leaderId = getLeaderId().orElse(getRandomPeerId());
        if (leaderId == getNodeId()) {  // I am the leader
            try {
                Set<String> keys = new BizurRun(request.getCorrelationId(), this).apiIterateKeys();
                return new KVIterateKeysResponse(true, null, keys).responseTo(request);
            } catch (Exception e) {
                logErrorForRequest(e, request);
                return new KVIterateKeysResponse(false, e, null).responseTo(request);
            }
        }
        // route to leader/peer
        LOGGER.debug("routing request={}, from={} to={}", request, getNodeId(), leaderId);
        return getRPC().iterateKeys(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    // TODO: refactor and get rid of these

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

    public Future<Void> delete(String key) {
        return exec(() -> {
            KVDeleteRequest request = new KVDeleteRequest(key);
            KVDeleteResponse response = delete(request);
            validateResponse(response, request);
            return null;
        });
    }

    public Future<Set<String>> iterateKeys() {
        return exec(() -> {
            KVIterateKeysRequest request = new KVIterateKeysRequest();
            KVIterateKeysResponse response = iterateKeys(request);
            validateResponse(response, request);
            return response.getKeys();
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

    private Optional<Integer> getLeaderId() {
        int leaderId;
        synchronized (bizurState) {
            leaderId = bizurState.getLeaderId();
        }
        return leaderId != -1
                ? Optional.of(leaderId)
                : Optional.empty();
    }

    private int getRandomPeerId() {
        int randPeerId = RNG.nextInt(getPeerSize());
        return randPeerId == getNodeId() ? (randPeerId + 1) % getPeerSize() : randPeerId;
    }

    Bucket getBucket(int index) {
        return bucketMap.computeIfAbsent(index, Bucket::new);
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

    BizurState getBizurStateUnprotected() {
        return bizurState;
    }

    int getPeerSize() {
        return peers.size();
    }

    int getNumBuckets() {
        return numBuckets;
    }

    /*----------------------------------------------------------------------------------
     * For testing
     * ----------------------------------------------------------------------------------*/

    Map<Integer, Bucket> getBucketMap() {
        return bucketMap;
    }
}
