package com.mboysan.consensus;

import com.mboysan.consensus.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class BizurRun {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurRun.class);

    private final String correlationId;
    private final BizurNode bizurNode;
    private final BizurState bizurState;

    BizurRun(BizurNode bizurNode) {
        this(Message.generateId(), bizurNode);
    }

    BizurRun(String correlationId, BizurNode bizurNode) {
        this.correlationId = correlationId;
        this.bizurNode = bizurNode;
        this.bizurState = bizurNode.getBizurStateUnprotected();
    }

    /*------------------------------ Method delegations & Helper functions ------------------------------*/

    private int getNodeId() {
        return bizurNode.getNodeId();
    }

    private void forEachPeerParallel(Consumer<BizurPeer> peerConsumer) {
        bizurNode.forEachPeerParallel(peerConsumer);
    }

    private BizurRPC getRPC() {
        return bizurNode.getRPC();
    }

    private void validateIAmTheLeader() throws BizurException {
        if (bizurState.getLeaderId() != getNodeId()) {
            throw new BizurException(String.format("node-%d is not the leader", getNodeId()));
        }
    }

    private int getNumBuckets() {
        return bizurNode.getNumBuckets();
    }

    private Bucket getBucket(int index) {
        return bizurNode.getBucket(index);
    }

    private boolean isMajorityAcked(int voteCount) {
        return voteCount > bizurNode.getPeerSize() / 2;
    }

    private int hashKey(String key) {
        return key.hashCode() % bizurNode.getNumBuckets();
    }

    private String contextInfo() {
        return String.format("[node-%d, corrId=%s]", getNodeId(), correlationId);
    }

    private void logPeerIOException(int peerId, Message request, IOException exception) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("{} - peer-{} IO exception for request={}, cause={}",
                    contextInfo(), peerId, request, exception.getMessage());
        }
    }

    /*------------------------------------------- Algorithms -------------------------------------------*/

    /*----------------------------------------------------------------------------------
     * Algorithm 1: Leader Election
     * ----------------------------------------------------------------------------------*/

    void startElection() {
        LOGGER.info("{} - starting new election", contextInfo());

        int electId = bizurState.incrementAndGetElectId();
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            PleaseVoteRequest request = new PleaseVoteRequest(electId)
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
            bizurState.setLeaderId(getNodeId());
            LOGGER.info("{} - I am leader", contextInfo());
        } else {
            bizurState.setLeaderId(-1);
        }
    }


    /*----------------------------------------------------------------------------------
     * Algorithm 2: Bucket Replication: Write
     * ----------------------------------------------------------------------------------*/

    private void write(Bucket bucket) throws BizurException {
        validateIAmTheLeader();
        int electId = bizurState.getElectId();
        int index = bucket.getIndex();

        bucket.setVerElectId(electId);
        bucket.incrementVerCounter();

        BucketView bucketView = bucket.createView();

        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaWriteRequest request = new ReplicaWriteRequest(index, bucketView)
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
            bizurState.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - write failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 3: Bucket Replication: Read
     * ----------------------------------------------------------------------------------*/

    private void read(Bucket bucket) throws BizurException {
        validateIAmTheLeader();

        int electId = bizurState.getElectId();

        ensureRecovery(bucket, electId);

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
            bizurState.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - read failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 4: Bucket Replication: Recovery
     * ----------------------------------------------------------------------------------*/

    private void ensureRecovery(Bucket bucket, int electId) throws BizurException {
        if (electId == bucket.getVerElectId()) {
            return;
        }

        int index = bucket.getIndex();
        AtomicReference<BucketView> maxVerBucketView = new AtomicReference<>(null);
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC().replicaRead(request);
                if (response.isAcked()) {
                    BucketView bucketView = response.getBucketView();
                    synchronized (maxVerBucketView) {
                        if (!maxVerBucketView.compareAndSet(null, bucketView)
                                && bucketView.compareTo(maxVerBucketView.get()) > 0) {
                            maxVerBucketView.set(bucketView);
                        }
                    }
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (isMajorityAcked(ackCount.get())) {
            BucketView bucketView = maxVerBucketView.get();
            bucket.setVerElectId(electId)
                    .setVerCounter(0)
                    .setBucketMap(bucketView.getBucketMap());
            write(bucket);
        } else {
            bizurState.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - could not ensure recovery of bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 5: Key-Value API
     * ----------------------------------------------------------------------------------*/

    String apiGet(String key) throws BizurException {
        Objects.requireNonNull(key);
        int index = hashKey(key);
        Bucket bucket = getBucket(index).lock();
        try {
            synchronized (bizurState) {
                read(bucket);
                return bucket.getOp(key);
            }
        } finally {
            bucket.unlock();
        }
    }

    void apiSet(String key, String value) throws BizurException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        int index = hashKey(key);
        Bucket bucket = getBucket(index).lock();
        try {
            synchronized (bizurState) {
                read(bucket);
                bucket.putOp(key, value);
                write(bucket);  // TODO: investigate if we need to revert if write fails
            }
        } finally {
            bucket.unlock();
        }
    }

    void apiDelete(String key) throws BizurException {
        Objects.requireNonNull(key);
        int index = hashKey(key);
        Bucket bucket = getBucket(index).lock();
        try {
            synchronized (bizurState) {
                read(bucket);
                bucket.removeOp(key);
                write(bucket);  // TODO: investigate if we need to revert if write fails
            }
        } finally {
            bucket.unlock();
        }
    }

    Set<String> apiIterateKeys() throws BizurException {
        Set<String> keys = new HashSet<>();
        for (int index = 0; index < getNumBuckets(); index++) {
            Bucket bucket = getBucket(index).lock();
            try {
                synchronized (bizurState) {
                    read(bucket);
                    keys.addAll(bucket.getKeySetOp());
                }
            } finally {
                bucket.unlock();
            }
        }
        return keys;
    }
}
