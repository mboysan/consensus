package com.mboysan.consensus;

import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.event.MeasurementEvent.MeasurementType;
import com.mboysan.consensus.message.CheckBizurIntegrityRequest;
import com.mboysan.consensus.message.CheckBizurIntegrityResponse;
import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;
import com.mboysan.consensus.util.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

final class BizurRun {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurRun.class);

    private final String correlationId;
    private final BizurNode bizurNode;

    BizurRun(BizurNode bizurNode) {
        this(Message.generateId(), bizurNode);
    }

    BizurRun(String correlationId, BizurNode bizurNode) {
        this.correlationId = correlationId;
        this.bizurNode = bizurNode;
    }

    /*------------------------------ Method delegations ------------------------------*/

    private int getNodeId() {
        return bizurNode.getNodeId();
    }

    private void forEachPeerParallel(Consumer<BizurPeer> peerConsumer) {
        bizurNode.forEachPeerParallel(peerConsumer);
    }

    private BizurClient getRPC() {
        return bizurNode.getRPC();
    }

    private boolean isMajorityAcked(int voteCount) {
        return voteCount >= countMajority();
    }

    private int countMajority() {
        return (bizurNode.getNumPeers() / 2) + 1;
    }

    private int hashKey(String key) {
        return bizurNode.hashKey(key);
    }

    private int rangeIndexForKey(String key) {
        return bizurNode.rangeIndexForKey(key);
    }

    private BucketRange getBucketRange(int rangeIndex) {
        return bizurNode.getBucketRange(rangeIndex);
    }

    private int getNumRanges() {
        return bizurNode.getNumRanges();
    }

    /*------------------------------ Helper functions ------------------------------*/

    private String contextInfo() {
        return String.format("[node-%d, corrId=%s]", getNodeId(), correlationId);
    }

    private void logPeerIOException(int peerId, Message request, IOException exception) {
        if (LOGGER.isErrorEnabled()) {
            String errMessage = exception.getMessage();
            Throwable cause = exception.getCause();
            LOGGER.error("{} - peer-{} IO exception for request={}, errMsg={}, cause={}",
                    contextInfo(), peerId, request, errMessage, (cause != null ? cause.getMessage() : null));
        }
    }

    private void validateIamTheLeader(BucketRange bucketRange) throws IllegalLeaderException {
        int bucketLeaderId = bucketRange.getLeaderId();
        if (bucketLeaderId == getNodeId()) {
            return;
        }
        throw new IllegalLeaderException(bucketLeaderId);
    }

    /*------------------------------------------- Algorithms -------------------------------------------*/

    /*----------------------------------------------------------------------------------
     * Algorithm 1: Leader Election
     * ----------------------------------------------------------------------------------*/

    void startElection(int bucketRangeIndex) {
        BucketRange range = getBucketRange(bucketRangeIndex).lock();
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("{} - starting new election on bucket rangeIdx={}", contextInfo(), range.getRangeIndex());
            }

            int electId = range.incrementAndGetElectId();
            AtomicInteger ackCount = new AtomicInteger(0);
            forEachPeerParallel(peer -> {
                PleaseVoteRequest request = new PleaseVoteRequest(range.getRangeIndex(), electId)
                        .setCorrelationId(correlationId)
                        .setSenderId(getNodeId())
                        .setReceiverId(peer.peerId);
                try {
                    PleaseVoteResponse response = getRPC().pleaseVote(request);
                    if (response.isAcked()) {
                        ackCount.incrementAndGet();
                    }
                } catch (IOException e) {
                    logPeerIOException(peer.peerId, request, e);
                }
            });
            if (isMajorityAcked(ackCount.get())) {
                range.setLeaderId(getNodeId());
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("{} - I am leader of bucket rangeIdx={}", contextInfo(), range.getRangeIndex());
                }
            } else {
                range.setLeaderId(-1);
            }
        } finally {
            range.unlock();
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 2: Bucket Replication: Write
     * ----------------------------------------------------------------------------------*/

    private void write(BucketRange range, Bucket bucket) throws BizurException {
        int electId = range.getElectId();
        int index = bucket.getIndex();

        bucket.setVerElectId(electId);
        bucket.incrementVerCounter();

        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaWriteRequest request = new ReplicaWriteRequest(index, bucket)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaWriteResponse response = getRPC().replicaWrite(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (!isMajorityAcked(ackCount.get())) {
            range.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - write failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 3: Bucket Replication: Read
     * ----------------------------------------------------------------------------------*/

    private void read(BucketRange range, Bucket bucket) throws BizurException {
        int electId = range.getElectId();

        ensureRecovery(range, bucket, electId);

        int index = bucket.getIndex();
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC().replicaRead(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (!isMajorityAcked(ackCount.get())) {
            range.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - read failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 4: Bucket Replication: Recovery
     * ----------------------------------------------------------------------------------*/

    private void ensureRecovery(BucketRange range, Bucket bucket, int electId) throws BizurException {
        if (electId == bucket.getVerElectId()) {
            return;
        }

        int index = bucket.getIndex();
        AtomicReference<Bucket> maxVerBucket = new AtomicReference<>(null);
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC().replicaRead(request);
                if (response.isAcked()) {
                    Bucket respBucket = response.getBucket();
                    synchronized (maxVerBucket) {
                        if (!maxVerBucket.compareAndSet(null, respBucket)
                                && respBucket.compareTo(maxVerBucket.get()) > 0) {
                            maxVerBucket.set(respBucket);
                        }
                    }
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (isMajorityAcked(ackCount.get())) {
            bucket.setVerElectId(electId);
            bucket.setVerCounter(0);
            bucket.setBucketMap(maxVerBucket.get().getBucketMap());
            write(range, bucket);
        } else {
            range.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - could not ensure recovery of bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 5: Key-Value API
     * ----------------------------------------------------------------------------------*/

    String apiGet(String key) throws BizurException {
        Objects.requireNonNull(key);
        int rangeIndex = rangeIndexForKey(key);
        BucketRange range = getBucketRange(rangeIndex).lock();
        try {
            validateIamTheLeader(range);
            int bucketIndex = hashKey(key);
            Bucket bucket = range.getBucket(bucketIndex);
            read(range, bucket);
            return bucket.getOp(key);
        } finally {
            range.unlock();
        }
    }

    void apiSet(String key, String value) throws BizurException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        int rangeIndex = rangeIndexForKey(key);
        BucketRange range = getBucketRange(rangeIndex).lock();
        try {
            validateIamTheLeader(range);
            int bucketIndex = hashKey(key);
            Bucket bucket = range.getBucket(bucketIndex);
            read(range, bucket);
            bucket.putOp(key, value);
            write(range, bucket);
        } finally {
            range.unlock();
        }
    }

    void apiDelete(String key) throws BizurException {
        Objects.requireNonNull(key);
        int rangeIndex = rangeIndexForKey(key);
        BucketRange range = getBucketRange(rangeIndex).lock();
        try {
            validateIamTheLeader(range);
            int bucketIndex = hashKey(key);
            Bucket bucket = range.getBucket(bucketIndex);
            read(range, bucket);
            bucket.removeOp(key);
            write(range, bucket);
        } finally {
            range.unlock();
        }
    }

    Set<String> collectKeys() throws BizurException {
        Set<String> keysIamResponsible = new HashSet<>();
        for (int i = 0; i < getNumRanges(); i++) {
            BucketRange range = getBucketRange(i).lock();
            try {
                if (range.getLeaderId() == getNodeId()) {
                    // I'm responsible from this range.
                    for (Bucket bucket : range.getBucketMap().values()) {
                        read(range, bucket);
                        keysIamResponsible.addAll(bucket.getKeySetOp());
                    }
                }
            } finally {
                range.unlock();
            }
        }
        return keysIamResponsible;
    }

    Set<String> apiIterateKeys() {
        Set<String> keySet = new HashSet<>();
        forEachPeerParallel(peer -> {
            CollectKeysRequest req = new CollectKeysRequest()
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                CollectKeysResponse response = getRPC().collectKeys(req);
                synchronized (keySet) {
                    keySet.addAll(response.keySet());
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, req, e);
            }
        });
        return keySet;
    }

    CheckBizurIntegrityResponse checkIntegrity(int level) throws IOException {
        CheckBizurIntegrityResponse thisNodeResponse = bizurNode.checkBizurIntegrity(
                new CheckBizurIntegrityRequest(level));

        String thisNodeIntegrityHash = thisNodeResponse.getIntegrityHash();
        String thisNodeState = thisNodeResponse.getState();

        Map<Integer, String> integrityHashes = new ConcurrentHashMap<>();
        Map<Integer, String> states = new ConcurrentHashMap<>();

        integrityHashes.put(getNodeId(), thisNodeIntegrityHash);
        states.put(getNodeId(), thisNodeState);

        forEachPeerParallel(peer -> {
            CheckBizurIntegrityRequest request = new CheckBizurIntegrityRequest(level)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                CheckBizurIntegrityResponse response = getRPC().checkBizurIntegrity(request);
                integrityHashes.put(peer.peerId, response.getIntegrityHash());
                states.put(peer.peerId, response.getState());
            } catch (IOException e) {
                LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
            }
        });

        Optional<String> majorityHash = HashUtil.findCommonHash(integrityHashes.values(), countMajority());

        LOGGER.info("node-{} integrityHash={}, majorityHash={}",
                getNodeId(), thisNodeIntegrityHash, majorityHash.orElse(null));

        return majorityHash.map(s ->
                        new CheckBizurIntegrityResponse(true, s, states.toString()))
                .orElseGet(() ->
                        new CheckBizurIntegrityResponse(false, thisNodeIntegrityHash, states.toString()));
    }

    void dumpMetricsAsync() {
        AtomicLong sizeOfKeys = new AtomicLong(0);
        AtomicLong sizeOfValues = new AtomicLong(0);
        AtomicLong totalSize = new AtomicLong(0);
        for (int i = 0; i < getNumRanges(); i++) {
            BucketRange range = getBucketRange(i).lock();
            try {
                range.getBucketMap().forEach((index, bucket) -> {
                    long sizeOfBucketKeys = bucket.getSizeOfKeys();
                    long sizeOfBucketValues = bucket.getSizeOfValues();
                    long bucketTotalSize = bucket.getTotalSize();

                    sizeOfKeys.addAndGet(sizeOfBucketKeys);
                    sizeOfValues.addAndGet(sizeOfBucketValues);
                    totalSize.addAndGet(bucketTotalSize);
                });
            } finally {
                range.unlock();
            }
        }
        fireMeasurementAsync(CoreConstants.Metrics.INSIGHTS_STORE_SIZE_OF_KEYS, sizeOfKeys.get());
        fireMeasurementAsync(CoreConstants.Metrics.INSIGHTS_STORE_SIZE_OF_VALUES, sizeOfValues.get());
        fireMeasurementAsync(CoreConstants.Metrics.INSIGHTS_STORE_SIZE_OF_TOTAL, totalSize.get());
    }

    private void fireMeasurementAsync(String name, long value) {
        EventManagerService.getInstance().fireAsync(new MeasurementEvent(MeasurementType.SAMPLE, name, value));
    }
}
