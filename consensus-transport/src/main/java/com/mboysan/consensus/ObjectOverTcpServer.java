package com.mboysan.consensus;

import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ObjectOverTcpServer extends ObjectIOServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectOverTcpClient.class);
    private final TcpTransportConfig config;
    private final ServerSocket serverSocket;

    public ObjectOverTcpServer(TcpTransportConfig config) throws IOException {
        this.config = config;
        this.serverSocket = new ServerSocket(config.port());
    }

    @Override
    ObjectOverTcpClient accept() throws IOException {
        Socket clientSocket = serverSocket.accept();
        clientSocket.setKeepAlive(true);
        if (config.socketSoTimeout() > 0) {
            clientSocket.setSoTimeout(config.socketSoTimeout());
        }
        return new ObjectOverTcpClient(clientSocket);
    }

    @Override
    public void close() {
        ShutdownUtil.close(LOGGER, serverSocket);
    }
}
