package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class BizurNode extends AbstractNode<BizurPeer> implements BizurRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurNode.class);

    private final BizurClient rpcClient;

    private final int numPeers;
    private final int numBuckets;
    private long updateIntervalMs;

    private final Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>();

    private final BizurState bizurState = new BizurState();

    public BizurNode(BizurConfig config, Transport transport) {
        super(config, transport);
        this.rpcClient = new BizurClient(transport);

        this.numPeers = config.numPeers();
        this.numBuckets = config.numBuckets();
        this.updateIntervalMs = config.updateIntervalMs();
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
        this.updateIntervalMs = updateIntervalMs * electId;
        LOGGER.info("node-{} modified updateIntervalMs={}", getNodeId(), updateIntervalMs);
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
        synchronized (bizurState) {
            bizurState.reset();
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithms
     * ----------------------------------------------------------------------------------*/

    @Override
    synchronized void update() {
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
        if (leaderId == -1 || !heartbeat(leaderId)) {   // if no leader or leader dead
            LOGGER.info("node-{} needs a new election", getNodeId());
            return true;
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
                    return new ReplicaReadResponse(true, bucket).responseTo(request);
                }
            }
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) {
        Bucket reqBucket = request.getBucket();
        Bucket bucket = getBucket(request.getBucketIndex()).lock();
        try {
            synchronized (bizurState) {
                if (reqBucket.getVerElectId() < bizurState.getVotedElectId()) {
                    return new ReplicaWriteResponse(false).responseTo(request);
                } else {
                    bizurState.setVotedElectId(reqBucket.getVerElectId())
                            .setLeaderId(request.getSenderId());     // "update" vote
                    bucket.setBucketMap(request.getBucket().getBucketMap());
                    return new ReplicaWriteResponse(true).responseTo(request);
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

        BucketRun.Result result;

        String key = request.getKey();
        int bucketIndex = hashKey(key);
        Bucket bucket = getBucket(bucketIndex).lock();
        try {
            BucketRun bucketRun = new BucketRun(request.getCorrelationId(), bucket, this);
            bucketRun.read();
            String value = bucket.getOp(key);

            result = BucketRun.Result.ok(value);
        } catch (BizurException e) {
            result = BucketRun.Result.error(e);
        } finally {
            bucket.unlock();
        }

        if (result.isOk()) {
            return new KVGetResponse(true, null, (String) result.payload()).responseTo(request);
        }
        Exception err = result.error();
        if (err instanceof IllegalLeaderException leaderErr) {
            int leaderId = leaderErr.getLeaderId();
            if (leaderId == -1) {
                // TODO: startElection



            }
            return route(request, leaderId);
        }
        return new KVGetResponse(false, err, null).responseTo(request);


        int leaderId;
        try {
            leaderId = getLeaderId().orElseThrow(() -> new IllegalStateException("leader unresolved"));
            if (leaderId == getNodeId()) {  // I am the leader
                String value = new BizurRun(request.getCorrelationId(), this).apiGet(request.getKey());
                return new KVGetResponse(true, null, value).responseTo(request);
            }
        } catch (IllegalLeaderException e) {
            //
        } catch (Exception e) {
            logErrorForRequest(e, request);
            return new KVGetResponse(false, e, null).responseTo(request);
        }
        // route to leader/peer
        logRequestRouting(request, getNodeId(), leaderId);
        return getRPC().get(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        validateAction();
        int leaderId;
        try {
            leaderId = getLeaderId().orElse(getRandomPeerId());
            if (leaderId == getNodeId()) {  // I am the leader
                new BizurRun(request.getCorrelationId(), this).apiSet(request.getKey(), request.getValue());
                return new KVSetResponse(true, null).responseTo(request);
            }
        } catch (Exception e) {
            logErrorForRequest(e, request);
            return new KVSetResponse(false, e).responseTo(request);
        }
        // route to leader/peer
        logRequestRouting(request, getNodeId(), leaderId);
        return getRPC().set(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) throws IOException {
        validateAction();
        int leaderId;
        try {
            leaderId = getLeaderId().orElseThrow(() -> new IllegalStateException("leader unresolved"));
            if (leaderId == getNodeId()) {  // I am the leader
                new BizurRun(request.getCorrelationId(), this).apiDelete(request.getKey());
                return new KVDeleteResponse(true, null).responseTo(request);
            }
        } catch (Exception e) {
            logErrorForRequest(e, request);
            return new KVDeleteResponse(false, e).responseTo(request);
        }
        // route to leader/peer
        logRequestRouting(request, getNodeId(), leaderId);
        return getRPC().delete(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException {
        validateAction();
        int leaderId;
        try {
            leaderId = getLeaderId().orElseThrow(() -> new IllegalStateException("leader unresolved"));
            if (leaderId == getNodeId()) {  // I am the leader
                Set<String> keys = new BizurRun(request.getCorrelationId(), this).apiIterateKeys();
                return new KVIterateKeysResponse(true, null, keys).responseTo(request);
            }
        } catch (Exception e) {
            logErrorForRequest(e, request);
            return new KVIterateKeysResponse(false, e, null).responseTo(request);
        }
        // route to leader/peer
        logRequestRouting(request, getNodeId(), leaderId);
        return getRPC().iterateKeys(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    private void enterElectionFlow(int bucketIndex, String correlationId) throws BizurException {
        // node = 0
        // peers = 3
        // buckets = 5
        // index = 0

        // 0 -> 0
        // 1 -> 0
        // 2 -> 1
        // 3 -> 1
        // 4 -> 2

        // buckets/peers = 5/3 = 2 per node
        // (index) % (buckets/peers)
        // 0 % 3 = 0
        // 1 % 3 = 1
        // 2 % 3 = 2
        // 3 % 3 = 0
        // 4 % 3 = 1

        int peersCount = peers.size();
        int supposedLeaderId = peers.get(bucketIndex % peersCount).peerId;
        int retriedNode = supposedLeaderId;
        do {
            Bucket bucket = getBucket(bucketIndex).lock();
            try {
                if (bucket.getLeaderId() == -1) {   // leader might've already been changed, hence this check
                    if (retriedNode == getNodeId()) {  // I am the supposed leader
                        BucketRun bucketRun = new BucketRun(correlationId, bucket, this);
                        bucketRun.startElection();
                    }
                }
                return; // success
            } catch (BizurException ignore) {

            } finally {
                bucket.unlock();
            }
            retriedNode = (retriedNode + 1) % peersCount;   // try next node

            if (heartbeat(retriedNode)) {
                continue;
            }


            try {
                // TODO: use semaphores
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BizurException(e.getMessage());
            }
        } while (retriedNode != supposedLeaderId);  // rolled over
        throw new BizurException("a leader could not be elected for bucket=" + bucketIndex);
    }

    private <T extends KVOperationResponse> T route(Message request, int receiverId) throws IOException {
        logRequestRouting(request, getNodeId(), receiverId);
        if (request instanceof KVGetRequest) {
            return getRPC().get(request.setReceiverId(receiverId).setSenderId(getNodeId()))
                    .responseTo(request);
        }
        if (request instanceof KVSetRequest) {
            return getRPC().set(request.setReceiverId(receiverId).setSenderId(getNodeId()))
                    .responseTo(request);
        }
        if (request instanceof KVDeleteRequest) {
            return getRPC().delete(request.setReceiverId(receiverId).setSenderId(getNodeId()))
                    .responseTo(request);
        }
        if (request instanceof KVIterateKeysRequest) {
            return getRPC().iterateKeys(request.setReceiverId(receiverId).setSenderId(getNodeId()))
                    .responseTo(request);
        }
        throw new IllegalArgumentException("unrecognized request=" + request.toString());
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private void resolveBucketLeader(int bucketIdx) {
        // 10
        // 3
        Bucket bucket = getBucket(bucketIdx).lock();
        try {

        } finally {
            bucket.unlock();
        }
    }

    private int hashKey(String key) {
        return key.hashCode() % getNumBuckets();
    }

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

    private void logRequestRouting(Message request, int from, int to) {
        LOGGER.debug("routing request={}, from={} to={}", request, from, to);
    }

    private void logErrorForRequest(Exception exception, Message request) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("err on node-{}: exception={}, request={}", getNodeId(), exception.getMessage(), request);
        }
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

    /*----------------------------------------------------------------------------------*/

    private static final class RunResult {
        private final Objects payload;

    }
}
