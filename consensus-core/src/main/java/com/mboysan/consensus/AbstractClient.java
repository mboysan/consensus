package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.util.Set;

abstract class AbstractClient implements RPCProtocol {
    private final Transport transport;

    AbstractClient(Transport transport) {
        this.transport = transport;
    }

    @Override
    public void onNodeListChanged(Set<Integer> serverIds) {
        throw new UnsupportedOperationException("this is relevant to only the server");
    }

    @Override
    public Message processRequest(Message request) {
        throw new UnsupportedOperationException("this is relevant to only the server");
    }

    Transport getTransport() {
        return transport;
    }
}
