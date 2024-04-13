package com.mboysan.consensus;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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

    //------------------------------- utils -------------------------------//

    BucketRange lock() {
        lock.lock();
        return this;
    }

    void unlock() {
        lock.unlock();
    }

    String getIntegrityHash() {
        return Integer.toHexString(Objects.hash(leaderId, bucketMap));
    }

    @Override
    public String toString() {
        return toInfoString();
    }

    public String toInfoString() {
        return toString(null);
    }

    public String toDebugString() {
        String bucketMapStr = bucketMap.values().stream()
                .map(Bucket::toDebugString)
                .collect(Collectors.joining(", "));
        return toString(bucketMapStr);
    }

    public String toTraceString() {
        String bucketMapStr = bucketMap.values().stream()
                .map(Bucket::toTraceString)
                .collect(Collectors.joining(", "));
        return toString(bucketMapStr);
    }

    private String toString(String bucketMapString) {
        String bucketMapStr = bucketMapString == null ? "" : ", bucketMap=" + bucketMapString;
        String bucketRangeStr =
                "BucketRange{" +
                        "rangeIndex=" + rangeIndex +
                        ", leaderId=" + leaderId +
                        ", electId=" + electId +
                        ", votedElectId=" + votedElectId +
                        "%s" +
                        ", integrityHash=" + getIntegrityHash() +
                        '}';
        return bucketRangeStr.formatted(bucketMapStr);
    }

    //------------------------------- for testing -------------------------------//

    Map<Integer, Bucket> getBucketMap() {
        return bucketMap;
    }
}
