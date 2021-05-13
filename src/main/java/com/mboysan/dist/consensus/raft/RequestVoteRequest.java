package com.mboysan.dist.consensus.raft;

import java.io.Serializable;

public class RequestVoteRequest implements Serializable {

    /**
     * candidate’s term
     */
    private final int term;
    /**
     * candidate requesting vote
     */
    private final int candidateId;
    /**
     * index of candidate’s last log entry (§5.4)
     */
    private final int lastLogIndex;
    /**
     * term of candidate’s last log entry (§5.4)
     */
    private final int lastLogTerm;

    public RequestVoteRequest(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return "RequestVoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
