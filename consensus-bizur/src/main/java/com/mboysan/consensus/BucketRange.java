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
        String bucketMapMetaStr = getBucketMapMetaData();
        String bucketMapStr = "N/A";
        return toString(bucketMapMetaStr, bucketMapStr);
    }

    public String toDebugString() {
        String bucketMapMetaStr = getBucketMapMetaData();
        String bucketMapStr = bucketMap.values().stream()
                .map(Bucket::toString)
                .collect(Collectors.joining(", "));
        return toString(bucketMapMetaStr, bucketMapStr);
    }

    private String getBucketMapMetaData() {
        int totalBuckets = bucketMap.size();
        int totalEntries = bucketMap.values().stream()
                .mapToInt(Bucket::getNumberOfEntries)
                .sum();
        String hashCodeOfBucketMaps = Integer.toHexString(
                bucketMap.values().stream()
                .map(Bucket::hashCodeOfBucketMap)
                .reduce(0, Objects::hash)
        );
        String hashCodeOfBucketMetadata = Integer.toHexString(
                bucketMap.values().stream()
                        .map(Bucket::hashCodeOfBucketMetadata)
                        .reduce(0, Objects::hash)
        );
        String hashCodeOfBucketMapAndBucketMetadata = Integer.toHexString(
                bucketMap.values().stream()
                        .map(Bucket::hashCodeOfBucketMapAndBucketMetadata)
                        .reduce(0, Objects::hash)
        );
        return "[" +
                "totalBuckets=" + totalBuckets +
                ", totalEntries=" + totalEntries +
                ", hashOfBucketMaps=" + hashCodeOfBucketMaps +
                ", hashOfBucketMapsMetadata=" + hashCodeOfBucketMetadata +
                ", hashOfBucketMapsAndMetadata=" + hashCodeOfBucketMapAndBucketMetadata +
                "]";
    }

    private String toString(String bucketMapMetaString, String bucketMapString) {
        return "BucketRange{" +
                "rangeIndex=" + rangeIndex +
                ", leaderId=" + leaderId +
                ", electId=" + electId +
                ", votedElectId=" + votedElectId +
                ", bucketMapMetadata=" + bucketMapMetaString +
                ", bucketMap=" + bucketMapString +
                ", integrityHash=" + getIntegrityHash() +
                '}';
    }

    //------------------------------- for testing -------------------------------//

    Map<Integer, Bucket> getBucketMap() {
        return bucketMap;
    }
}
