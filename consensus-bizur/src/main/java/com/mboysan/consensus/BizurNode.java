package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
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

        return CompletableFuture.completedFuture(null);
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
        forEachPeerParallel(peer -> {
            if (peer.numBuckets() > 0) {
                HeartbeatRequest request = new HeartbeatRequest(System.currentTimeMillis());
                try {
                    HeartbeatResponse response = getRPC(getTransport()).heartbeat(request);
                    if (LOGGER.isDebugEnabled()) {
                        long elapsed = response.getSendTimeMs() - request.getSendTimeMs();
                        LOGGER.debug("peer-{} heartbeat elapsed={}", peer.peerId, elapsed);
                    }
                } catch (IOException e) {
                    LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
                    // peer is dead, lets start the election for all its buckets.
                    Set<Bucket> buckets = peer.getAndRemoveBuckets();
                    for (Bucket bucket : buckets) {
                        Bucket lockedBucket = bucket.lock();
                        try {
                            startElection(lockedBucket);
                        } finally {
                            lockedBucket.unlock();
                        }
                    }
                }
            }
        });
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 1: Leader Election
     * ----------------------------------------------------------------------------------*/

    private void startElection(Bucket bucket) {
        LOGGER.info("node-{} starting new election for bucket={}", getNodeId(), bucket.getIndex());

        AtomicInteger ackCount = new AtomicInteger(0);
        int electId = bucket.incrementAndGetElectId();
        forEachPeerParallel(peer -> {
            PleaseVoteRequest request = new PleaseVoteRequest(bucket.getIndex(), electId)
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
        if (isMajorityAcked(ackCount.get())) {
            bucket.setLeaderId(getNodeId());
            LOGGER.info("node-{} thinks it's leader of bucket={}", getNodeId(), bucket.getIndex());
        }
    }

    private boolean isElectionNeeded(Bucket bucket) {
        long currentTime = getTimers().currentTime();
        if (currentTime >= electionTime) {
            electionTime = currentTime + electionTimeoutMs;
            boolean isElectionNeeded = !isBucketLeader(bucket) && !bucket.seenLeader();
            bucket.setSeenLeader(false);
            if (isElectionNeeded) {
                LOGGER.info("node-{} needs a new election", getNodeId());
            }
            return isElectionNeeded;
        }
        return false;
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 2: Bucket Replication: Write
     * ----------------------------------------------------------------------------------*/

    private boolean write(Bucket lockedBucket) {
        lockedBucket.incrementAndGetVerCounter();

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
            lockedBucket.setLeaderId(-1);   // step down
            return false;
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 3: Bucket Replication: Read
     * ----------------------------------------------------------------------------------*/

    private void read(Bucket lockedBucket) throws BizurException {
        if (!ensureRecovery(lockedBucket)) {
            throw new BizurException("Bucket recovery failed, index=" + lockedBucket.getIndex());
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(lockedBucket.getIndex(), lockedBucket.getElectId())
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
            lockedBucket.setLeaderId(-1);   // step down
            throw new BizurException(String.format("node-%d stepped down from leadership of bucket=%d",
                    getNodeId(), lockedBucket.getIndex()));
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 4: Bucket Replication: Recovery
     * ----------------------------------------------------------------------------------*/

    private boolean ensureRecovery(Bucket lockedBucket) {
        if (lockedBucket.getElectId() == lockedBucket.getVerElectId()) {
            return true;
        }
        AtomicReference<BucketView> maxVerBucketView = new AtomicReference<>(null);
        AtomicInteger ackCount = new AtomicInteger(0);
        forEachPeerParallel(peer -> {
            ReplicaReadRequest request = new ReplicaReadRequest(lockedBucket.getIndex(), lockedBucket.getElectId())
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
            lockedBucket.setVerElectId(bucketView.getVerElectId());
            lockedBucket.setVerCounter(0);
            lockedBucket.setBucketMap(bucketView.getBucketMap());
            return write(lockedBucket);
        } else {
            lockedBucket.setLeaderId(-1);
            return false;
        }
    }

    /*----------------------------------------------------------------------------------
     * Algorithm 5: Key-Value API
     * ----------------------------------------------------------------------------------*/

    private String get(Bucket lockedBucket, String key) throws BizurException {
        read(lockedBucket);
        return lockedBucket.getOp(key);
    }

    private boolean set(Bucket lockedBucket, String key, String value) throws BizurException {
        read(lockedBucket);
        lockedBucket.putOp(key, value); // TODO: investigate if we need to revert if write fails
        return write(lockedBucket);
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
        Bucket bucket = getBucket(request.getBucketIndex()).lock();
        try {
            if (request.getElectId() > bucket.getVotedElectId()) {
                bucket.setVotedElectId(request.getElectId());
                bucket.setLeaderId(request.getSenderId()); // "update" vote
                updateVote(bucket, request.getSenderId());
                bucket.setSeenLeader(true);
                return new PleaseVoteResponse(true).responseTo(request);
            } else if (request.getElectId() == bucket.getVotedElectId() && request.getSenderId() == bucket.getLeaderId()) {
                bucket.setSeenLeader(true);
                return new PleaseVoteResponse(true).responseTo(request);
            }
            return new PleaseVoteResponse(false).responseTo(request);
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public ReplicaReadResponse replicaRead(ReplicaReadRequest request) {
        Bucket bucket = getBucket(request.getBucketIndex()).lock();
        try {
            if (bucket.getElectId() < bucket.getVotedElectId()) {
                return new ReplicaReadResponse(false, null).responseTo(request);
            } else {
                bucket.setVotedElectId(request.getElectId());
                bucket.setLeaderId(request.getSenderId());  // "update" vote
                updateVote(bucket, request.getSenderId());
                BucketView bucketView = bucket.createView();
                return new ReplicaReadResponse(true, bucketView).responseTo(request);
            }
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) {
        Bucket bucket = getBucket(request.getBucketIndex()).lock();
        try {
            BucketView bucketView = request.getBucketView();
            if (bucketView.getVerElectId() < bucket.getVotedElectId()) {
                return new ReplicaWriteResponse(false);
            } else {
                bucket.setVotedElectId(bucketView.getVerElectId());
                bucket.setLeaderId(request.getSenderId());   // "update" vote
                updateVote(bucket, request.getSenderId());
                bucket.setBucketMap(bucketView.getBucketMap());
                return new ReplicaWriteResponse(true);
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
        int bucketIndex = hashKey(request.getKey());
        leaderCheck(bucketIndex);

        int leaderId;
        Bucket bucket = getBucket(bucketIndex).lock();
        try {
            if (isBucketLeader(bucket)) {
                String value = get(bucket, request.getKey());
                return new KVGetResponse(true, value).responseTo(request);
            } else {
                leaderId = bucket.getLeaderId();
            }
        } catch (BizurException e) {
            LOGGER.error(e.getMessage(), e);
            return new KVGetResponse(false, null).responseTo(request);
        } finally {
            bucket.unlock();
        }
        return getRPC(getTransport()).get(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        int bucketIndex = hashKey(request.getKey());
        leaderCheck(bucketIndex);

        int leaderId;
        Bucket bucket = getBucket(bucketIndex).lock();
        try {
            if (isBucketLeader(bucket)) {
                boolean success = set(bucket, request.getKey(), request.getValue());
                return new KVSetResponse(success).responseTo(request);
            } else {
                leaderId = bucket.getLeaderId();
            }
        } catch (BizurException e) {
            LOGGER.error(e.getMessage(), e);
            return new KVSetResponse(false).responseTo(request);
        } finally {
            bucket.unlock();
        }
        return getRPC(getTransport()).set(request.setReceiverId(leaderId).setSenderId(getNodeId()))
                .responseTo(request);
    }

    private void leaderCheck(int bucketIndex) {
        while (true) {
            Bucket bucket = getBucket(bucketIndex).lock();
            try {
                if (bucketHasLeader(bucket)) {
                    break;
                }
                startElection(bucket);
                if (bucketHasLeader(bucket)) {
                    break;
                }
            } finally {
                bucket.unlock();
            }
            // lets wait a bit, maybe a leader is already elected
            getTimers().sleep(electionTimeoutMs);
        }
    }

    /*----------------------------------------------------------------------------------
     * Helper Functions
     * ----------------------------------------------------------------------------------*/

    private void updateVote(Bucket bucket, int newLeaderId) {
        int bucketIdx = bucket.getIndex();
        int oldLeaderId = bucket.getLeaderId();
        if (oldLeaderId == newLeaderId) {
            return;
        }
        peers.get(oldLeaderId).removeBucket(bucketIdx);
        peers.get(newLeaderId).putBucket(bucketIdx, bucket);
    }

    private Bucket getBucket(int index) {
        return bucketMap.computeIfAbsent(index, Bucket::new);
    }

    private boolean bucketHasLeader(Bucket bucket) {
        return bucket.getLeaderId() != -1;
    }

    private boolean isBucketLeader(Bucket bucket) {
        return bucket.getLeaderId() == getNodeId();
    }

    private boolean isMajorityAcked(int voteCount) {
        return voteCount + 1 > peers.size() / 2;
    }

    private int hashKey(String key) {
        return key.hashCode() % numBuckets;
    }
}
