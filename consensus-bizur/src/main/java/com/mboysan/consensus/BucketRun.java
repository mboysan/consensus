package com.mboysan.consensus;

import com.mboysan.consensus.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class BucketRun {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRun.class);

    private final String correlationId;
    private final Bucket bucket;
    private final BizurNode bizurNode;

    BucketRun(Bucket lockedBucket, BizurNode bizurNode) {
        this(Message.generateId(), lockedBucket, bizurNode);
    }

    BucketRun(String correlationId, Bucket lockedBucket, BizurNode bizurNode) {
        this.correlationId = correlationId;
        this.bucket = lockedBucket;
        this.bizurNode = bizurNode;
    }

    /*------------------------------ Method delegations & Helper functions ------------------------------*/

    private int getNodeId() {
        return bizurNode.getNodeId();
    }

    private void forEachPeerParallel(Consumer<BizurPeer> peerConsumer) {
        bizurNode.forEachPeerParallel(peerConsumer);
    }

    private BizurRPC getRPC(int peerId) {
        if (peerId == getNodeId()) {
            return bizurNode;   // allows calling real methods without using IO communication.
        }
        return bizurNode.getRPC();
    }

    private void validateIAmTheLeader() throws IllegalLeaderException {
        if (bucket.getLeaderId() != getNodeId()) {
            throw new IllegalLeaderException(bucket.getLeaderId());
        }
    }

    private boolean isMajorityAcked(int voteCount) {
        return voteCount > bizurNode.getPeerSize() / 2;
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

    /*----------------------------------------------------------------------------------
     * Algorithm 1: Leader Election
     * ----------------------------------------------------------------------------------*/

    void startElection() throws BizurException {
        LOGGER.info("{} - starting new election", contextInfo());

        int index = bucket.getIndex();
        int electId = bucket.incrementAndGetElectId();
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            PleaseVoteRequest request = new PleaseVoteRequest(index, electId)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                PleaseVoteResponse response = getRPC(peer.peerId).pleaseVote(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (isMajorityAcked(ackCount.get())) {
            bucket.setLeaderId(getNodeId());
            LOGGER.info("{} - I am leader", contextInfo());
        } else {
            throw new BizurException(String.format("%s - election failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 2: Bucket Replication: Write
     * ----------------------------------------------------------------------------------*/

    void write() throws BizurException {
        validateIAmTheLeader();

        int electId = bucket.getElectId();
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
                ReplicaWriteResponse response = getRPC(peer.peerId).replicaWrite(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (!isMajorityAcked(ackCount.get())) {
            bucket.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - write failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 3: Bucket Replication: Read
     * ----------------------------------------------------------------------------------*/

    void read() throws BizurException {
        validateIAmTheLeader();

        int electId = bucket.getElectId();

        ensureRecovery(electId);

        int index = bucket.getIndex();
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(index, electId)
                    .setCorrelationId(correlationId)
                    .setSenderId(getNodeId())
                    .setReceiverId(peer.peerId);
            try {
                ReplicaReadResponse response = getRPC(peer.peerId).replicaRead(request);
                if (response.isAcked()) {
                    ackCount.incrementAndGet();
                }
            } catch (IOException e) {
                logPeerIOException(peer.peerId, request, e);
            }
        });
        if (!isMajorityAcked(ackCount.get())) {
            bucket.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - read failed for bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 4: Bucket Replication: Recovery
     * ----------------------------------------------------------------------------------*/

    private void ensureRecovery(int electId) throws BizurException, IllegalLeaderException {
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
                ReplicaReadResponse response = getRPC(peer.peerId).replicaRead(request);
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
            write();
        } else {
            bucket.setLeaderId(-1);  // step down
            throw new BizurException(String.format("%s - could not ensure recovery of bucket index=%d, electId=%d",
                    contextInfo(), index, electId));
        }
    }

    /*----------------------------------------------------------------------------------*/

    static class Result {

        private final boolean isOk;
        private final Object payload;
        private final BizurException error;

        private Result(boolean isOk, Object payload, BizurException error) {
            this.isOk = isOk;
            this.payload = payload;
            this.error = error;
        }

        public boolean isOk() {
            return isOk;
        }

        public Object payload() {
            return payload;
        }

        public BizurException error() {
            return error;
        }

        static Result ok(Object payload) {
            return new Result(true, payload, null);
        }

        static Result error(BizurException err) {
            return new Result(false, null, err);
        }
    }
}
