package com.mboysan.consensus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class BizurPeer extends AbstractPeer {

    private final Map<Integer, Bucket> responsibleBuckets = new HashMap<>();

    BizurPeer(int peerId) {
        super(peerId);
    }

    synchronized void putBucket(int bucketIdx, Bucket bucket) {
        responsibleBuckets.putIfAbsent(bucketIdx, bucket);
    }

    synchronized Bucket removeBucket(int bucketIdx) {
        return responsibleBuckets.remove(bucketIdx);
    }

    synchronized Set<Bucket> getAndRemoveBuckets() {
        Set<Bucket> buckets = new HashSet<>(responsibleBuckets.values());
        responsibleBuckets.clear();
        return buckets;
    }

    synchronized int numBuckets() {
        return responsibleBuckets.size();
    }

    @Override
    synchronized void reset() {
        responsibleBuckets.clear();
    }
}
