package com.mboysan.consensus.message;

public class PleaseVoteRequest extends Message {

    private final int bucketRangeIndex;
    private final int electId;

    public PleaseVoteRequest(int bucketRangeIndex, int electId) {
        this.bucketRangeIndex = bucketRangeIndex;
        this.electId = electId;
    }

    public int getBucketRangeIndex() {
        return bucketRangeIndex;
    }

    public int getElectId() {
        return electId;
    }

    @Override
    public String toString() {
        return "PleaseVoteRequest{" +
                "bucketRangeIndex=" + bucketRangeIndex +
                ", electId=" + electId +
                '}';
    }
}
