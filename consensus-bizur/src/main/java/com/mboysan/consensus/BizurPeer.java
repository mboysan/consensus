package com.mboysan.consensus;

class BizurPeer extends AbstractPeer {

    BizurPeer(int peerId) {
        super(peerId);
    }

    @Override
    synchronized void reset() {
        // nothing to do
    }
}
