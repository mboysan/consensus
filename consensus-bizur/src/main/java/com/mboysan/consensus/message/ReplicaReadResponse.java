package com.mboysan.consensus.message;

import com.mboysan.consensus.Bucket;

public class ReplicaReadResponse extends Message {

    private final boolean acked;
    private final Bucket bucketView;

    public ReplicaReadResponse(boolean acked, Bucket bucketView) {
        this.acked = acked;
        this.bucketView = bucketView;
    }

    public boolean isAcked() {
        return acked;
    }

    public Bucket getBucket() {
        return bucketView;
    }

    @Override
    public String toString() {
        return "ReplicaReadResponse{" +
                "acked=" + acked +
                "} " + super.toString();
    }
}
