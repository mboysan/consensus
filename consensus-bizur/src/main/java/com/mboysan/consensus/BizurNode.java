package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.message.HeartbeatRequest;
import com.mboysan.consensus.message.HeartbeatResponse;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVOperationResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;
import com.mboysan.consensus.util.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BizurNode extends AbstractNode<BizurPeer> implements BizurRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurNode.class);

    private final BizurClient rpcClient;

    private final Lock updateLock = new ReentrantLock();

    private final int numBuckets;
    private final long updateIntervalMs;
    private long electionTimeoutMs;
    private long nextElectionTime;

    private final Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>();

    private final BizurState bizurState = new BizurState();

    public BizurNode(BizurConfig config, Transport transport) {
        super(config, transport);
        this.rpcClient = new BizurClient(transport);

        this.numBuckets = config.numBuckets();
        this.updateIntervalMs = config.updateIntervalMs();
        this.electionTimeoutMs = config.electionTimeoutMs();
    }

    @Override
    BizurRPC getRPC() {
        return rpcClient;
    }

    @Override
    BizurPeer createPeer(int peerId) {
        return new BizurPeer(peerId);
    }

    @Override
    Future<Void> startNode() {
        int electId = (getNodeId() % (peers.size() + 1)) + 1;
        this.electionTimeoutMs = electionTimeoutMs * electId;
        this.nextElectionTime = getTimers().currentTime() + electionTimeoutMs;
        LOGGER.info("node-{} modified electionTimeoutMs={}, nextElectionTime={}",
                getNodeId(), electionTimeoutMs, nextElectionTime);
        getTimers().schedule("updateTimer-node" + getNodeId(), this::tryUpdate, updateIntervalMs, updateIntervalMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                synchronized (bizurState) {
                    if (bizurState.getLeaderId() != -1) {
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
        if (leaderId == getNodeId()) {
            return; // I am the leader, no need to take action
        }
        if (isElectionNeeded(leaderId)) {
            new BizurRun(this).startElection();
        }
    }

    private boolean isElectionNeeded(int leaderId) {
        long currentTime = getTimers().currentTime();
        if (currentTime >= nextElectionTime) {
            nextElectionTime = currentTime + electionTimeoutMs;
            if (leaderId == -1 || !heartbeat(leaderId)) {   // if no leader or leader dead
                LOGGER.info("node-{} needs a new election", getNodeId());
                return true;
            }
        }
        return false;
    }

    private boolean heartbeat(int leaderId) {
        HeartbeatRequest request = new HeartbeatRequest(System.currentTimeMillis())
                .setSenderId(getNodeId())
                .setReceiverId(leaderId);
        try {
            HeartbeatResponse response = getRPC().heartbeat(request);
            if (LOGGER.isTraceEnabled()) {
                long elapsed = response.getSendTimeMs() - request.getSendTimeMs();
                LOGGER.trace("peer-{} heartbeat elapsed={}", leaderId, elapsed);
            }
            return true;
        } catch (IOException e) {
            LOGGER.error("peer-{} IO exception for request={}, cause={}", leaderId, request, e.getMessage());
            return false;
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
        return Math.abs((int) System.currentTimeMillis()) % getPeerSize();
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
