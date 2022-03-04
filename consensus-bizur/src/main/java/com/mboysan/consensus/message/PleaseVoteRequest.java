package com.mboysan.consensus.message;

public class PleaseVoteRequest extends Message {

    private final int bucketIndex;
    private final int electId;

    public PleaseVoteRequest(int bucketIndex, int electId) {
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
        return "PleaseVoteRequest{" +
                "bucketIndex=" + bucketIndex +
                ", electId=" + electId +
                '}';
    }
}
