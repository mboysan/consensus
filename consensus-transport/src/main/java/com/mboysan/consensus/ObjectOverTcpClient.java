package com.mboysan.consensus;

import com.mboysan.consensus.configuration.TcpDestination;
import com.mboysan.consensus.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

class ObjectOverTcpClient extends ObjectIOClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectOverTcpClient.class);
    private final Socket socket;
    private final ObjectOutputStream os;
    private final ObjectInputStream is;

    ObjectOverTcpClient(TcpDestination destination) throws IOException {
        this(new Socket(destination.ip(), destination.port()));
    }

    ObjectOverTcpClient(Socket socket) throws IOException {
        this.socket = socket;
        this.os = new ObjectOutputStream(socket.getOutputStream());
        this.is = new ObjectInputStream(socket.getInputStream());
    }

    @Override
    Serializable readObject() throws IOException, ClassNotFoundException {
        synchronized (is) {
            return (Serializable) is.readObject(); // blocks
        }
    }

    @Override
    void writeObject(Serializable object) throws IOException {
        synchronized (os) {
            os.writeObject(object);
            os.flush();
            os.reset();
        }
    }

    @Override
    public void close() {
        synchronized (os) {
            ShutdownUtil.close(LOGGER, os);
        }
        synchronized (is) {
            ShutdownUtil.close(LOGGER, is);
        }
        ShutdownUtil.close(LOGGER, socket);
    }
}
