package com.mboysan.consensus;

public class PleaseVoteRequest extends Message {

    private final int bucketIndex;
    private final int electId;

    public PleaseVoteRequest(int bucketIndex, int electId) {
        this.bucketIndex = bucketIndex;
        this.electId = electId;
    }

    public int getElectId() {
        return electId;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    @Override
    public String toString() {
        return "PleaseVoteRequest{" +
                "bucketIndex=" + bucketIndex +
                ", electId=" + electId +
                "} " + super.toString();
    }
}
