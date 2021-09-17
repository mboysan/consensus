package com.mboysan.consensus;

import java.util.Set;
import java.util.concurrent.Future;

abstract class AbstractClient implements RPCProtocol {
    private final Transport transport;

    public AbstractClient(Transport transport) {
        this.transport = transport;
    }

    @Override
    public void onNodeListChanged(Set<Integer> serverIds) {
        throw new UnsupportedOperationException("this is relevant to only the RaftServer");
    }

    @Override
    public Future<Void> start() {
        throw new UnsupportedOperationException("no start() for client");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("no shutdown() for client");
    }

    public Transport getTransport() {
        return transport;
    }
}
