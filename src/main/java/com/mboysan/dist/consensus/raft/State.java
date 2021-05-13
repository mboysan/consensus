package com.mboysan.dist.consensus.raft;

import java.util.Stack;

public class State {

    // Persistent state on all servers:

    /** latest term server has seen (initialized to 0 on first boot, increases monotonically */
    int currentTerm;
    /** candidateId that received vote in current term (or null if none), null=-1 */
    int votedFor = -1;
    /** log entries; each entry contains command for state machine, and term when entry was received by leader
     * (first index is 1) */
//    List<RaftLog> raftLog = new ArrayList<>();
    Stack<LogEntry> raftLog = new Stack<>();

    //Volatile state on all servers:
    /** index of highest log entry known to be committed (initialized to 0, increases monotonically) */
    int commitIndex;
    /** index of highest log entry applied to state machine (initialized to 0, increases monotonically) */
    int lastApplied;

    //Volatile state on leaders:

    /** for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) */
    int nextIndex[];
    /** for each server, index of highest log entry known to be replicated on server (initialized to 0,
     * increases monotonically) */
    int matchIndex[];

}
