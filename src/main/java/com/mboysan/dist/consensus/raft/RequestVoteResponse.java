package com.mboysan.dist.consensus.raft;

import java.io.Serializable;

public class RequestVoteResponse implements Serializable {

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
                '}';
    }
}
