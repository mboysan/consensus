package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.message.BizurKVDeleteRequest;
import com.mboysan.consensus.message.BizurKVDeleteResponse;
import com.mboysan.consensus.message.BizurKVGetRequest;
import com.mboysan.consensus.message.BizurKVGetResponse;
import com.mboysan.consensus.message.BizurKVIterateKeysRequest;
import com.mboysan.consensus.message.BizurKVIterateKeysResponse;
import com.mboysan.consensus.message.BizurKVOperationResponse;
import com.mboysan.consensus.message.BizurKVSetRequest;
import com.mboysan.consensus.message.BizurKVSetResponse;
import com.mboysan.consensus.message.CheckBizurIntegrityRequest;
import com.mboysan.consensus.message.CheckBizurIntegrityResponse;
import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.HeartbeatRequest;
import com.mboysan.consensus.message.HeartbeatResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;
import com.mboysan.consensus.util.RngUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

    // individual ranges will be locked
    private final Map<Integer, BucketRange> bucketRanges = new ConcurrentHashMap<>();

    public BizurNode(BizurConfig config, Transport transport) {
        super(config, transport);
        this.rpcClient = new BizurClient(transport);

        this.numPeers = config.numPeers() > 0
                ? config.numPeers()
                : Objects.requireNonNull(transport.getDestinationNodeIds(), "numPeers not resolved").size();
        this.numBuckets = config.numBuckets() <= 0 ? Integer.MAX_VALUE : config.numBuckets();
        this.updateIntervalMs = config.updateIntervalMs();
    }

    @Override
    BizurClient rpc() {
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
        this.updateIntervalMs = RngUtil.nextLong(this.updateIntervalMs, this.updateIntervalMs * 2);
        LOGGER.info("node-{} modified updateIntervalMs={}", getNodeId(), updateIntervalMs);
        getScheduler().schedule("updateTimer-node" + getNodeId(), this::update, updateIntervalMs, updateIntervalMs);

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
                getScheduler().sleep(updateIntervalMs);
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
        LOGGER.debug("node-{} update timeout, time={}", getNodeId(), getScheduler().currentTime());
        for (int rangeIndex = 0; rangeIndex < getNumRanges(); rangeIndex++) {
            int currentRangeLeader = getRangeLeader(rangeIndex);

            if (currentRangeLeader == getNodeId()) {
                // I'm the leader of the range, all good
                continue;
            }

            int supposedRangeLeader = nodeIdForRangeIndex(rangeIndex);

            boolean iAmSupposedLeader;
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
            HeartbeatResponse response = rpc().heartbeat(request);
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
        return new HeartbeatResponse(System.currentTimeMillis());
    }

    @Override
    public PleaseVoteResponse pleaseVote(PleaseVoteRequest request) {
        BucketRange range = getBucketRange(request.getBucketRangeIndex()).lock();
        try {
            if (request.getElectId() > range.getVotedElectId()) {
                range.setVotedElectId(request.getElectId());
                range.setLeaderId(request.getSenderId());     // "update" vote
                return new PleaseVoteResponse(true);
            } else if (request.getElectId() == range.getVotedElectId()
                    && request.getSenderId() == range.getLeaderId()) {
                return new PleaseVoteResponse(true);
            }
            return new PleaseVoteResponse(false);
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
                return new ReplicaReadResponse(false, null);
            } else {
                range.setVotedElectId(electId);
                range.setLeaderId(source);    // "update" vote
                return new ReplicaReadResponse(true, bucket);
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
                return new ReplicaWriteResponse(false);
            } else {
                range.setVotedElectId(bucketReceived.getVerElectId());
                range.setLeaderId(request.getSenderId());     // "update" vote
                // finally, local buckets[bucket.index] ← bucket
                bucket.setVerElectId(bucketReceived.getVerElectId());
                bucket.setVerCounter(bucketReceived.getVerCounter());
                bucket.setBucketMap(bucketReceived.getBucketMap());
                return new ReplicaWriteResponse(true);
            }
        } finally {
            range.unlock();
        }
    }

    @Override
    public CollectKeysResponse collectKeys(CollectKeysRequest request) {
        try {
            Set<String> myKeys = new BizurRun(request.getCorrelationId(), this).collectKeys();
            return new CollectKeysResponse(true, null, myKeys);
        } catch (BizurException e) {
            return new CollectKeysResponse(false, e, null);
        }
    }

    /*----------------------------------------------------------------------------------
     * Public RPC Commands
     * ----------------------------------------------------------------------------------*/

    @Override
    public BizurKVGetResponse get(BizurKVGetRequest request) throws IOException {
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
    public BizurKVSetResponse set(BizurKVSetRequest request) throws IOException {
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
    public BizurKVDeleteResponse delete(BizurKVDeleteRequest request) throws IOException {
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
    public BizurKVIterateKeysResponse iterateKeys(BizurKVIterateKeysRequest request) throws IOException {
        validateAction();
        Set<String> keySet = new BizurRun(request.getCorrelationId(), this).apiIterateKeys();
        return response(request, true, null, keySet);
    }

    @Override
    public CheckBizurIntegrityResponse checkBizurIntegrity(CheckBizurIntegrityRequest request) throws IOException {
        validateAction();
        if (request.isRoutingNeeded()) {
            return routeMessage(request);
        }
        switch (request.getLevel()) {
            case CoreConstants.StateLevels.INFO_STATE,
                 CoreConstants.StateLevels.DEBUG_STATE -> {
                Map<Integer, String> states = new HashMap<>();
                int finalIntegrityHash = 0;
                for (int rangeIndex = 0; rangeIndex < getNumRanges(); rangeIndex++) {
                    BucketRange range = getBucketRange(rangeIndex).lock();
                    try {
                        finalIntegrityHash = Objects.hash(finalIntegrityHash, range.getIntegrityHash());
                        final String state = CoreConstants.StateLevels.INFO_STATE == request.getLevel()
                                ? range.toInfoString()
                                : range.toDebugString();
                        states.put(rangeIndex, state);
                    } finally {
                        range.unlock();
                    }
                }
                return new CheckBizurIntegrityResponse(
                        true, Integer.toHexString(finalIntegrityHash), states.toString());
            }
            case CoreConstants.StateLevels.INFO_STATE_FROM_ALL,
                 CoreConstants.StateLevels.DEBUG_STATE_FROM_ALL -> {
                int levelOverride = CoreConstants.StateLevels.INFO_STATE_FROM_ALL == request.getLevel()
                        ? CoreConstants.StateLevels.INFO_STATE
                        : CoreConstants.StateLevels.DEBUG_STATE;
                return new BizurRun(request.getCorrelationId(), this).checkIntegrity(levelOverride);
            }
            default -> throw new IOException("unsupported level=" + request.getLevel());
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

    private <T extends BizurKVOperationResponse> T route(Message request, int receiverId) throws IOException {
        if (receiverId == -1) {
            BizurException err = new BizurException("leader unresolved");
            logErrorForRequest(err, request);
            return response(request, false, err, null);
        }
        return routeMessage(request, receiverId);
    }

    @SuppressWarnings("unchecked")
    private <T extends BizurKVOperationResponse> T response(
            Message request, boolean success, Exception err, Object payload)
    {
        if (request instanceof BizurKVGetRequest) {
            return (T) new BizurKVGetResponse(success, err, (String) payload);
        }
        if (request instanceof BizurKVSetRequest) {
            return (T) new BizurKVSetResponse(success, err);
        }
        if (request instanceof BizurKVDeleteRequest) {
            return (T) new BizurKVDeleteResponse(success, err);
        }
        if (request instanceof BizurKVIterateKeysRequest) {
            return (T) new BizurKVIterateKeysResponse(success, err, (Set<String>) payload);
        }
        throw new IllegalArgumentException("unrecognized request=" + request.toString());
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    int hashKey(String key) {
        return Math.abs(key.hashCode()) % numBuckets;
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

    void dumpMetricsAsync() {
        new BizurRun(this).dumpMetricsAsync();
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
