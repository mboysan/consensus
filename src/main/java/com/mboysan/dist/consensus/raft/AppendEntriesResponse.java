package com.mboysan.dist.consensus.raft;

import java.io.Serializable;

public class AppendEntriesResponse implements Serializable {

    /**
     * for leader to update itself
     */
    private final int term;
    /**
     * true if follower contained entry matching prevLogIndex and prevLogTerm
     */
    private final boolean success;

    private final int matchIndex;

    public AppendEntriesResponse(int term, boolean success, int matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public AppendEntriesResponse(int term, boolean success) {
        this(term, success, -1);
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
