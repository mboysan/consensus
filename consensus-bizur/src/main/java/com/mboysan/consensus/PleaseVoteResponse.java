package com.mboysan.consensus;

public class PleaseVoteResponse extends Message {

    private final boolean acked;

    public PleaseVoteResponse(boolean acked) {
        this.acked = acked;
    }

    public boolean isAcked() {
        return acked;
    }

    @Override
    public String toString() {
        return "PleaseVoteResponse{" +
                "acked=" + acked +
                "} " + super.toString();
    }
}
