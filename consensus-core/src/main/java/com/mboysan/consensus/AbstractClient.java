package com.mboysan.consensus;

abstract class AbstractClient {
    private final Transport transport;

    AbstractClient(Transport transport) {
        this.transport = transport;
    }

    Transport getTransport() {
        return transport;
    }
}
