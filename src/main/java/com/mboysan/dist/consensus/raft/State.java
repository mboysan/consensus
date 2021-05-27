package com.mboysan.dist.consensus.raft;

import static com.mboysan.dist.consensus.raft.State.Role.FOLLOWER;

public class State {

    enum Role {
        CANDIDATE, FOLLOWER, LEADER
    }

    // Persistent state on all servers:

    /** latest term server has seen (initialized to 0 on first boot, increases monotonically */
    int currentTerm = 0;
    /** candidateId that received vote in current term (or null if none), null=-1 */
    int votedFor = -1;
    /** log entries; each entry contains command for state machine, and term when entry was received by leader
     * (first index is 1) */
    RaftLog raftLog = new RaftLog();

    //Volatile state on all servers:
    /** index of highest log entry known to be committed (initialized to 0, increases monotonically) */
    int commitIndex = 0;
    /** index of highest log entry applied to state machine (initialized to 0, increases monotonically) */
    int lastApplied = 0;

    int leaderId = -1;
    boolean isElectionNeeded = false;
    Role role = FOLLOWER;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof State)) return false;

        State state = (State) o;

        if (currentTerm != state.currentTerm) return false;
        if (votedFor != state.votedFor) return false;
        if (commitIndex != state.commitIndex) return false;
        if (lastApplied != state.lastApplied) return false;
        if (leaderId != state.leaderId) return false;
        if (isElectionNeeded != state.isElectionNeeded) return false;
        if (!raftLog.equals(state.raftLog)) return false;
        return role == state.role;
    }

    @Override
    public int hashCode() {
        int result = currentTerm;
        result = 31 * result + votedFor;
        result = 31 * result + raftLog.hashCode();
        result = 31 * result + commitIndex;
        result = 31 * result + lastApplied;
        result = 31 * result + leaderId;
        result = 31 * result + (isElectionNeeded ? 1 : 0);
        result = 31 * result + role.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "State{" +
                "currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", raftLog=" + raftLog +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", leaderId=" + leaderId +
                ", isElectionNeeded=" + isElectionNeeded +
                ", role=" + role +
                '}';
    }
}
