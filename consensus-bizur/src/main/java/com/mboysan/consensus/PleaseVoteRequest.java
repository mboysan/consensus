package com.mboysan.consensus;

public class PleaseVoteRequest extends Message {

    private final int electId;

    public PleaseVoteRequest(int electId) {
        this.electId = electId;
    }

    public int getElectId() {
        return electId;
    }

    @Override
    public String toString() {
        return "PleaseVoteRequest{" +
                "electId=" + electId +
                "} " + super.toString();
    }
}
