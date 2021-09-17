package com.mboysan.consensus;

public class ReplicaReadResponse extends Message {

    private final boolean acked;
    private final BucketView bucketView;

    public ReplicaReadResponse(boolean acked, BucketView bucketView) {
        this.acked = acked;
        this.bucketView = bucketView;
    }

    public boolean isAcked() {
        return acked;
    }

    public BucketView getBucketView() {
        return bucketView;
    }

    @Override
    public String toString() {
        return "ReplicaReadResponse{" +
                "acked=" + acked +
                "} " + super.toString();
    }
}
