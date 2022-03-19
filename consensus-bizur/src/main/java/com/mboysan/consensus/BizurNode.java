package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class BizurNode extends AbstractNode<BizurPeer> implements BizurRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurNode.class);

    private static final String SEED = UUID.randomUUID().toString();
    static {
        LOGGER.info("seed={}", SEED);
    }
    private static final SecureRandom RNG = new SecureRandom(SEED.getBytes());

    private final BizurClient rpcClient;

    private final int numPeers;
    private final int numBuckets;
    private long updateIntervalMs;

    // individual ranges will be locked
    private final Map<Integer, BucketRange> bucketRanges = new ConcurrentHashMap<>();

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
        this.updateIntervalMs = RNG.nextLong(this.updateIntervalMs, this.updateIntervalMs * 2);
        LOGGER.info("node-{} modified updateIntervalMs={}", getNodeId(), updateIntervalMs);
        getTimers().schedule("updateTimer-node" + getNodeId(), this::update, updateIntervalMs, updateIntervalMs);

        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                boolean allRangesHaveCorrectLeader = true;
                for (int rangeIndex = 0; rangeIndex < getNumRanges(); rangeIndex++) {
                    if (getRangeLeader(rangeIndex) != nodeIdForRangeIndex(rangeIndex)) {
                        allRangesHaveCorrectLeader = false;
                    }
                }
                if (allRangesHaveCorrectLeader) {
                    return null;
                }
                getTimers().sleep(updateIntervalMs);
            }
        });
    }

    @Override
    void shutdownNode() {
        for (int rangeIndex = 0; rangeIndex < getNumRanges(); rangeIndex++) {
            BucketRange range = getBucketRange(rangeIndex).lock();
            try {
                range.reset();
            } finally {
                range.unlock();
            }
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithms
     * ----------------------------------------------------------------------------------*/

    @Override
    synchronized void update() {
        LOGGER.debug("node-{} update timeout, time={}", getNodeId(), getTimers().currentTime());
        for (int rangeIndex = 0; rangeIndex < getNumRanges(); rangeIndex++) {
            int currentRangeLeader = getRangeLeader(rangeIndex);

            if (currentRangeLeader == getNodeId()) {
                // I'm the leader of the range, all good
                continue;
            }

            int supposedRangeLeader = nodeIdForRangeIndex(rangeIndex);

            boolean iAmSupposedLeader = false;
            boolean noRangeLeader = false;
            boolean currentLeaderDead = false;
            boolean supposedLeaderDead = false;
            if ((iAmSupposedLeader = supposedRangeLeader == getNodeId())
                    || (noRangeLeader = currentRangeLeader == -1)
                    || ((currentLeaderDead = !heartbeat(currentRangeLeader))
                        && (supposedLeaderDead = !heartbeat(supposedRangeLeader)))) {
                // If I am the supposed leader OR there's no leader OR current and supposed leader unreachable,
                // then, I'll try to be the new leader
                String correlationId = Message.generateId();
                LOGGER.info("node-{} needs a new election on bucket rangeIdx={}, " +
                                "reason=[iAmSupposedLeader={}, noRangeLeader={}, currentLeaderDead={}, supposedLeaderDead={}], corrId={}",
                        getNodeId(), rangeIndex,
                        iAmSupposedLeader, noRangeLeader, currentLeaderDead, supposedLeaderDead,
                        correlationId);
                new BizurRun(correlationId, this).startElection(rangeIndex);
            }
        }
    }

    private int getRangeLeader(int rangeIndex) {
        BucketRange range = getBucketRange(rangeIndex).lock();
        try {
            return range.getLeaderId();
        } finally {
            range.unlock();
        }
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
        BucketRange range = getBucketRange(request.getBucketRangeIndex()).lock();
        try {
            if (request.getElectId() > range.getVotedElectId()) {
                range.setVotedElectId(request.getElectId());
                range.setLeaderId(request.getSenderId());     // "update" vote
                return new PleaseVoteResponse(true).responseTo(request);
            } else if (request.getElectId() == range.getVotedElectId()
                    && request.getSenderId() == range.getLeaderId()) {
                return new PleaseVoteResponse(true).responseTo(request);
            }
            return new PleaseVoteResponse(false).responseTo(request);
        } finally {
            range.unlock();
        }
    }

    @Override
    public ReplicaReadResponse replicaRead(ReplicaReadRequest request) {
        int bucketIndex = request.getBucketIndex();
        int electId = request.getElectId();
        int source = request.getSenderId();
        BucketRange range = getBucketRange(rangeIndexForBucketIndex(bucketIndex)).lock();
        try {
            Bucket bucket = range.getBucket(bucketIndex);
            if (electId < range.getVotedElectId()) {
                return new ReplicaReadResponse(false, null).responseTo(request);
            } else {
                range.setVotedElectId(electId);
                range.setLeaderId(source);    // "update" vote
                return new ReplicaReadResponse(true, bucket).responseTo(request);
            }
        } finally {
            range.unlock();
        }
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) {
        BucketRange range = getBucketRange(rangeIndexForBucketIndex(request.getBucketIndex())).lock();
        try {
            Bucket bucket = range.getBucket(request.getBucketIndex());
            Bucket bucketReceived = request.getBucket();
            if (bucketReceived.getVerElectId() < range.getVotedElectId()) {
                return new ReplicaWriteResponse(false).responseTo(request);
            } else {
                range.setVotedElectId(bucketReceived.getVerElectId());
                range.setLeaderId(request.getSenderId());     // "update" vote
                bucket.setBucketMap(request.getBucket().getBucketMap());
                return new ReplicaWriteResponse(true).responseTo(request);
            }
        } finally {
            range.unlock();
        }
    }

    @Override
    public CollectKeysResponse collectKeys(CollectKeysRequest request) {
        Set<String> keysIamResponsible = new HashSet<>();
        for (Integer rangeIndex : request.rangeIndexes()) {
            BucketRange range = getBucketRange(rangeIndex).lock();
            int rangeLeader = range.getLeaderId();
            try {
                if (range.getLeaderId() != getNodeId()) {
                    return new CollectKeysResponse(
                            false, new IllegalLeaderException(rangeLeader), null).responseTo(request);
                }
                keysIamResponsible.addAll(range.getKeysOfAllBuckets());
            } finally {
                range.unlock();
            }
        }
        return new CollectKeysResponse(true, null, keysIamResponsible).responseTo(request);
    }

    /*----------------------------------------------------------------------------------
     * Public RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public KVGetResponse get(KVGetRequest request) throws IOException {
        validateAction();
        try {
            String value = new BizurRun(request.getCorrelationId(), this).apiGet(request.getKey());
            return response(request, true, null, value);
        } catch (IllegalLeaderException e) {
            return route(request, e.getLeaderId());
        } catch (BizurException e) {
            logErrorForRequest(e, request);
            return response(request, false, e, null);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        validateAction();
        try {
            new BizurRun(request.getCorrelationId(), this).apiSet(request.getKey(), request.getValue());
            return response(request, true, null, null);
        } catch (IllegalLeaderException e) {
            return route(request, e.getLeaderId());
        } catch (BizurException e) {
            logErrorForRequest(e, request);
            return response(request, false, e, null);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) throws IOException {
        validateAction();
        try {
            new BizurRun(request.getCorrelationId(), this).apiDelete(request.getKey());
            return response(request, true, null, null);
        } catch (IllegalLeaderException e) {
            return route(request, e.getLeaderId());
        } catch (BizurException e) {
            logErrorForRequest(e, request);
            return response(request, false, e, null);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException {
        validateAction();
        try {
            Set<String> keySet = new BizurRun(request.getCorrelationId(), this).apiIterateKeys();
            return response(request, true, null, keySet);
        } catch (IllegalLeaderException e) {
            return route(request, e.getLeaderId());
        } catch (BizurException e) {
            logErrorForRequest(e, request);
            return response(request, false, e, null);
        }
    }

    private <T extends KVOperationResponse> T route(Message request, int receiverId) throws IOException {
        logRequestRouting(request, getNodeId(), receiverId);
        if (receiverId == -1) {
            BizurException err = new BizurException("leader unresolved");
            logErrorForRequest(err, request);
            return response(request, false, err, null);
        }
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

    @SuppressWarnings("unchecked")
    private <T extends KVOperationResponse> T response(
            Message request, boolean success, Exception err, Object payload)
    {
        if (request instanceof  KVGetRequest) {
            return new KVGetResponse(success, err, (String) payload).responseTo(request);
        }
        if (request instanceof  KVSetRequest) {
            return new KVSetResponse(success, err).responseTo(request);
        }
        if (request instanceof  KVDeleteRequest) {
            return new KVDeleteResponse(success, err).responseTo(request);
        }
        if (request instanceof  KVIterateKeysRequest) {
            return new KVIterateKeysResponse(success, err, (Set<String>) payload).responseTo(request);
        }
        throw new IllegalArgumentException("unrecognized request=" + request.toString());
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    int hashKey(String key) {
        return key.hashCode() % numBuckets;
    }

    int rangeIndexForKey(String key) {
        int bucketIndex = hashKey(key);
        return rangeIndexForBucketIndex(bucketIndex);
    }

    int rangeIndexForBucketIndex(int bucketIndex) {
        return bucketIndex % getNumRanges();
    }

    int nodeIdForRangeIndex(int rangeIndex) {
        return rangeIndex % getNumRanges();
    }

    BucketRange getBucketRange(int rangeIndex) {
        return bucketRanges.computeIfAbsent(rangeIndex, BucketRange::new);
    }

    int getNumPeers() {
        return numPeers;
    }

    int getNumRanges() {
        return Math.min(numPeers, numBuckets);
    }

    private void logRequestRouting(Message request, int from, int to) {
        LOGGER.debug("routing request={}, from={} to={}", request, from, to);
    }

    private void logErrorForRequest(Exception exception, Message request) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("err on node-{}: exception={}, request={}", getNodeId(), exception.getMessage(), request);
        }
    }

    /*----------------------------------------------------------------------------------
     * For testing
     * ----------------------------------------------------------------------------------*/

    Map<Integer, BucketRange> getBucketRanges() {
        return bucketRanges;
    }
}
