package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpDestination;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

abstract class ObjectIOClient implements AutoCloseable {
    abstract Serializable readObject() throws IOException, ClassNotFoundException;
    abstract void writeObject(Serializable object) throws IOException;

    static ObjectIOClient create(Destination destination) throws IOException {
        Objects.requireNonNull(destination);
        if (destination instanceof TcpDestination dest) {
            return new ObjectOverTcpClient(dest);
        }
        throw new IllegalArgumentException("unsupported destination type = " + destination);
    }
}
