package com.mboysan.consensus;

import java.util.HashMap;
import java.util.Map;

/**
 * numPeers = 3
 * numBuckets = 1000
 * range = 3
 *
 * numPeers = 1
 * numBuckets = 1000
 * range = 1
 *
 * numPeers = 5
 * numBuckets = 5
 * range = 5
 *
 * numPeers = 5
 * numBuckets = 1
 * range = 1
 *
 * ----- Range resolution:
 * key = abc
 * hash = 123456
 * belongs to bucket (bucketIndex) = hash % numBuckets
 * belongs to range (rangeIndex) = ranges.get(bucketIndex % peersCount);
 *
 * ----- how many ranges?
 * (numBuckets % peersCount) + 1 ?
 *
 * - example-1:
 * numBuckets = 1
 * numPeers = 5
 * numRanges = 1
 *
 * - example-2:
 * numBuckets = 5
 * numPeers = 3
 * numRanges = 3
 */
class BucketRange {

    private final Map<Integer, Bucket> bucketMap = new HashMap<>();

    private int leaderId = -1;
    private int electId = 0;
    private int votedElectId = -1;

    BucketRange() {
        reset();
    }

    //----------------------------------------------------
    // state
    // ----------------------------------------------------

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

}
