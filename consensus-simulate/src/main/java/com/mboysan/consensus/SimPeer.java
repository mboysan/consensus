package com.mboysan.consensus;

class SimPeer extends AbstractPeer {
    SimPeer(int peerId) {
        super(peerId);
    }

    @Override
    void reset() {
        // no-op
    }
}
