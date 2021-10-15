package com.mboysan.consensus;

abstract class AbstractPeer {
    final int peerId;

    AbstractPeer(int peerId) {
        this.peerId = peerId;
        reset();
    }

    abstract void reset();
}
