package com.mboysan.consensus.message;

import com.mboysan.consensus.Bucket;

public class ReplicaWriteRequest extends Message {

    private final int bucketIndex;
    private final Bucket bucket;

    public ReplicaWriteRequest(int bucketIndex, Bucket bucket) {
        this.bucketIndex = bucketIndex;
        this.bucket = bucket;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    public Bucket getBucket() {
        return bucket;
    }

    @Override
    public String toString() {
        return "ReplicaWriteRequest{" +
                "bucketIndex=" + bucketIndex +
                ", bucketView=" + bucket +
                "} " + super.toString();
    }
}
