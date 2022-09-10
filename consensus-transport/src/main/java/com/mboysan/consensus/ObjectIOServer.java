package com.mboysan.consensus;

import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.configuration.TransportConfig;

import java.io.IOException;
import java.util.Objects;

abstract class ObjectIOServer implements AutoCloseable {
    abstract ObjectIOClient accept() throws IOException;

    static ObjectIOServer create(TransportConfig config) throws IOException {
        Objects.requireNonNull(config);
        if (config instanceof TcpTransportConfig conf) {
            return new ObjectOverTcpServer(conf);
        }
        throw new IllegalArgumentException("unsupported transport config = " + config);
    }
}
