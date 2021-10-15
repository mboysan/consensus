package com.mboysan.consensus;

public class ReplicaReadRequest extends Message {
    private final int bucketIndex;
    private final int electId;

    public ReplicaReadRequest(int bucketIndex, int electId) {
        this.bucketIndex = bucketIndex;
        this.electId = electId;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    public int getElectId() {
        return electId;
    }

    @Override
    public String toString() {
        return "ReplicaReadRequest{" +
                "bucketIndex=" + bucketIndex +
                ", electId=" + electId +
                "} " + super.toString();
    }
}
