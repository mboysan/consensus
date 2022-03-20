package com.mboysan.consensus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

class BucketRange {

    private final ReentrantLock lock = new ReentrantLock();

    private final int rangeIndex;

    private int leaderId = -1;
    private int electId = 0;
    private int votedElectId = -1;

    private final Map<Integer, Bucket> bucketMap = new HashMap<>();

    BucketRange(int rangeIndex) {
        this.rangeIndex = rangeIndex;
        reset();
    }

    public int getRangeIndex() {
        return rangeIndex;
    }

    //------------------------------- state -------------------------------//

    void reset() {
        leaderId = -1;
        electId = 0;
        votedElectId = -1;
    }

    int getLeaderId() {
        return leaderId;
    }

    void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    int getElectId() {
        return electId;
    }

    int incrementAndGetElectId() {
        return ++this.electId;
    }

    int getVotedElectId() {
        return votedElectId;
    }

    void setVotedElectId(int votedElectId) {
        this.votedElectId = votedElectId;
    }

    //------------------------------- bucket -------------------------------//

    Bucket getBucket(int index) {
        return bucketMap.computeIfAbsent(index, Bucket::new);
    }

    Set<String> getKeysOfAllBuckets() {
        Set<String> keys = new HashSet<>();
        for (Bucket bucket : bucketMap.values()) {
            keys.addAll(bucket.getKeySetOp());
        }
        return keys;
    }

    //------------------------------- utils -------------------------------//

    BucketRange lock() {
        lock.lock();
        return this;
    }

    void unlock() {
        lock.unlock();
    }

    //------------------------------- for testing -------------------------------//

    Map<Integer, Bucket> getBucketMap() {
        return bucketMap;
    }
}
