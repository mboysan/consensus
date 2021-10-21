package com.mboysan.consensus.message;

public class RequestVoteResponse extends Message {

    /**
     * currentTerm, for candidate to update itself
     */
    private final int term;
    /**
     * true means candidate received vote
     */
    private final boolean voteGranted;

    public RequestVoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                "} " + super.toString();
    }
}
