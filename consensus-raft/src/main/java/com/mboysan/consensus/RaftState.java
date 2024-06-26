package com.mboysan.consensus;

import java.util.Objects;

class RaftState {

    enum Role {
        CANDIDATE, FOLLOWER, LEADER
    }

    // Persistent state on all servers:

    /**
     * latest term server has seen (initialized to 0 on first boot, increases monotonically
     */
    int currentTerm = 0;
    /**
     * candidateId that received vote in current term (or null if none), null=-1
     */
    int votedFor = -1;
    /**
     * log entries; each entry contains command for state machine, and term when entry was received by leader
     * (first index is 1)
     */
    RaftLog raftLog = new RaftLog();

    //Volatile state on all servers:
    /**
     * index of highest log entry known to be committed (initialized to 0, increases monotonically)
     */
    int commitIndex = -1;
    /**
     * index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     */
    int lastApplied = -1;

    int leaderId = -1;
    boolean seenLeader = false;
    Role role = Role.FOLLOWER;

    void reset() {
        this.currentTerm = 0;
        this.votedFor = -1;
        this.raftLog.reset();
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.leaderId = -1;
        this.seenLeader = false;
        this.role = Role.FOLLOWER;
    }

    String getIntegrityHash() {
        return Integer.toHexString(Objects.hash(currentTerm, commitIndex, leaderId, raftLog));
    }

    @Override
    public String toString() {
        return toInfoString();
    }

    public String toInfoString() {
        return toString(raftLog.toInfoString());
    }

    public String toDebugString() {
        return toString(raftLog.toDebugString());
    }

    private String toString(String raftLogString) {
        return "State{" +
                "currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", raftLog=" + raftLogString +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", leaderId=" + leaderId +
                ", seenLeader=" + seenLeader +
                ", role=" + role +
                ", integrityHash=" + getIntegrityHash() +
                '}';
    }
}
