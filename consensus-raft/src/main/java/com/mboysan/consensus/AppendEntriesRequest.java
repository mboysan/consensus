package com.mboysan.consensus;

import java.util.List;

public class AppendEntriesRequest extends Message {
    /**
     * leader’s term
     */
    private final int term;
    /**
     * so follower can redirect clients
     */
    private final int leaderId;
    /**
     * index of log entry immediately preceding new ones
     */
    private final int prevLogIndex;
    /**
     * term of prevLogIndex entry
     */
    private final int prevLogTerm;
    /**
     * log entries to store (empty for heartbeat; may send more than one for efficiency)
     */
    private final List<LogEntry> entries;
    /**
     * leader’s commitIndex
     */
    private final int leaderCommit;

    public AppendEntriesRequest(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + entries +
                ", leaderCommit=" + leaderCommit +
                "} " + super.toString();
    }
}
