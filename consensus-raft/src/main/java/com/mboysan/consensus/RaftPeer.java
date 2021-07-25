package com.mboysan.consensus;

class RaftPeer extends AbstractPeer {
    boolean voteGranted;
    /** for each server, index of highest log entry known to be replicated on server (initialized to 0,
     * increases monotonically) */
    int matchIndex;
    /** for each server, index of the next log entry to send to that server (initialized to leader last
     * log index, i.e. unlike Raft paper states which is last log index + 1) */
    int nextIndex;

    public RaftPeer(int peerId) {
        super(peerId);
    }

    @Override
    void reset() {
        voteGranted = false;
        matchIndex = -1;
        nextIndex = 0;
    }
}
