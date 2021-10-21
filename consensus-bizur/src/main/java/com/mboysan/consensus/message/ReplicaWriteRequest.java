package com.mboysan.consensus.message;

import com.mboysan.consensus.BucketView;

public class ReplicaWriteRequest extends Message {

    private final int bucketIndex;
    private final BucketView bucketView;

    public ReplicaWriteRequest(int bucketIndex, BucketView bucketView) {
        this.bucketIndex = bucketIndex;
        this.bucketView = bucketView;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    public BucketView getBucketView() {
        return bucketView;
    }

    @Override
    public String toString() {
        return "ReplicaWriteRequest{" +
                "bucketIndex=" + bucketIndex +
                ", bucketView=" + bucketView +
                "} " + super.toString();
    }
}
