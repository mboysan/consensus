package com.mboysan.consensus.network;

import com.mboysan.consensus.EventManagerService;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.TcpDestination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.ShutdownUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

public class VanillaTcpServerTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpServerTransport.class);

    private final int port;
    private final Map<Integer, TcpDestination> destinations;
    private final int nodeIdOrPort;
    private final Map<String, ClientHandler> clientHandlers = new ConcurrentHashMap<>();

    private ServerSocket serverSocket;
    private ExecutorService clientHandlerExecutor;

    private volatile boolean isRunning = false;
    private UnaryOperator<Message> messageProcessor;

    /**
     * Id of the node that this transport is responsible from
     */
    private final VanillaTcpClientTransport clientTransport;
    private final int socketSoTimeout;

    public VanillaTcpServerTransport(TcpTransportConfig config) {
        LOGGER.info("server transport config={}", config);
        this.port = config.port();
        this.destinations = config.destinations();
        this.nodeIdOrPort = destinations.values().stream()
                .filter(dest -> dest.port() == port)
                .mapToInt(TcpDestination::nodeId)
                .findFirst().orElse(port);
        this.clientTransport = new VanillaTcpClientTransport(config, nodeIdOrPort);
        this.socketSoTimeout = config.socketSoTimeout();
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public void registerMessageProcessor(UnaryOperator<Message> messageProcessor) {
        if (this.messageProcessor != null && !this.messageProcessor.equals(messageProcessor)) {  // for restarts
            throw new IllegalStateException("request processor already registered");
        }
        this.messageProcessor = messageProcessor;
    }

    @Override
    public synchronized void start() {
        if (isRunning) {
            return;
        }
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Thread serverThread = new Thread(this::serverThreadLoop, "server-" + nodeIdOrPort);
        serverThread.setDaemon(false);
        this.clientHandlerExecutor = Executors.newCachedThreadPool(
                new BasicThreadFactory.Builder()
                        .namingPattern("server-" + nodeIdOrPort +"-handler-" + "%d")
                        .daemon(false)
                        .build());

        serverThread.start();
        clientTransport.start();
        isRunning = true;
    }

    private void serverThreadLoop() {
        int clientCount = 0;
        while (isRunning) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setKeepAlive(true);
                if (socketSoTimeout > 0) {
                    clientSocket.setSoTimeout(socketSoTimeout);
                }
                ClientHandler clientHandler = new ClientHandler(clientSocket, clientCount);
                clientHandlers.put(clientCount + "", clientHandler);
                clientHandlerExecutor.submit(clientHandler);
                ++clientCount;
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    @Override
    public Set<Integer> getDestinationNodeIds() {
        return Collections.unmodifiableSet(destinations.keySet());
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) {
        return clientTransport.sendRecvAsync(message);
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        if (message.getSenderId() == message.getReceiverId()) {
            LOGGER.debug("IN (self): {}", message);
            return messageProcessor.apply(message);
        }
        return clientTransport.sendRecv(message);
    }

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        ShutdownUtil.close(LOGGER, serverSocket);
        ShutdownUtil.shutdown(LOGGER, clientHandlerExecutor);
        clientHandlers.forEach((s, ch) -> ShutdownUtil.shutdown(LOGGER, ch::shutdown));
        clientHandlers.clear();
        ShutdownUtil.shutdown(LOGGER, clientTransport::shutdown);
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning && clientTransport.verifyShutdown();
    }

    private final class ClientHandler implements Runnable {
        private volatile boolean isRunning = true;
        private final Socket socket;
        private final ObjectOutputStream os;
        private final ObjectInputStream is;
        private final ExecutorService requestExecutor;

        private ClientHandler(Socket socket, int handlerId) throws IOException {
            this.socket = socket;
            this.os = new ObjectOutputStream(socket.getOutputStream());
            this.is = new ObjectInputStream(socket.getInputStream());
            this.requestExecutor = Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder()
                            .namingPattern("server-" + nodeIdOrPort +"-handler-" + handlerId + "-exec-" + "%d")
                            .daemon(false)
                            .build());
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Message request = (Message) is.readObject();
                    LOGGER.debug("IN (request): {}", request);
                    sampleReceive(request);

                    // we allow multiple requests from the same client, hence, we don't block on processing the
                    // request and writing the response back to the client.
                    requestExecutor.submit(() -> {
                        Message response = messageProcessor.apply(request);
                        try {
                            synchronized (os) {
                                LOGGER.debug("OUT (response): {}", response);
                                os.writeObject(response);
                                os.flush();
                                os.reset();
                                sampleSend(response);
                            }
                        } catch (IOException e) {
                            LOGGER.error(e.getMessage());
                            shutdown();
                        }
                    });
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error(e.getMessage());
                    shutdown();
                }
            }
        }

        synchronized void shutdown() {
            if (!isRunning) {
                return;
            }
            isRunning = false;
            synchronized (os) {
                ShutdownUtil.close(LOGGER, os);
            }
            synchronized (is) {
                ShutdownUtil.close(LOGGER, is);
            }
            ShutdownUtil.close(LOGGER, socket);
            ShutdownUtil.shutdown(LOGGER, requestExecutor);
        }
    }

    private static void sampleSend(Message message) {
        sample("insights.tcp.server.send.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sampleReceive(Message message) {
        sample("insights.tcp.server.receive.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sample(String name, Message message) {
        if (EventManagerService.getInstance().listenerExists(MeasurementEvent.class)) {
            // fire async measurement event
            EventManagerService.getInstance().fireAsync(
                    new MeasurementEvent(MeasurementEvent.MeasurementType.SAMPLE, name, message));
        }
    }
}
