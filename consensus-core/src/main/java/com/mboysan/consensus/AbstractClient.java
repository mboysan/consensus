package com.mboysan.consensus;

import java.util.Set;

abstract class AbstractClient implements RPCProtocol {
    private final Transport transport;

    AbstractClient(Transport transport) {
        this.transport = transport;
    }

    @Override
    public void onNodeListChanged(Set<Integer> serverIds) {
        throw new UnsupportedOperationException("this is relevant to only the RaftServer");
    }

    Transport getTransport() {
        return transport;
    }
}
