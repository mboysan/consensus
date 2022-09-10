package com.mboysan.consensus;

import com.mboysan.consensus.configuration.TransportConfig;
import com.mboysan.consensus.configuration.TransportConfigHelper;
import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.ShutdownUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

class ServerTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerTransport.class);

    private final TransportConfig config;
    private final int serverId;
    private final Map<String, ClientHandler> clientHandlers = new ConcurrentHashMap<>();
    private ObjectIOServer ioServer;
    private ExecutorService clientHandlerExecutor;

    private volatile boolean isRunning = false;
    private UnaryOperator<Message> messageProcessor;

    /**
     * Id of the node that this transport is responsible from
     */
    private final ClientTransport clientTransport;

    ServerTransport(TransportConfig config) {
        LOGGER.info("server transport config={}", config);
        this.config = config;
        this.serverId = TransportConfigHelper.resolveId(config);
        this.clientTransport = TransportFactory.createClientTransport(config, serverId);
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
            this.ioServer = ObjectIOServer.create(config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Thread serverThread = new Thread(this::serverThreadLoop, "server-" + serverId);
        serverThread.setDaemon(false);
        this.clientHandlerExecutor = Executors.newCachedThreadPool(
                new BasicThreadFactory.Builder()
                        .namingPattern("server-" + serverId +"-handler-" + "%d")
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
                ObjectIOClient ioClient = ioServer.accept();
                ClientHandler clientHandler = new ClientHandler(ioClient, clientCount);
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
        return TransportConfigHelper.resolveDestinationNodeIds(config);
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
        ShutdownUtil.close(LOGGER, ioServer);
        ShutdownUtil.shutdown(LOGGER, clientHandlerExecutor);
        clientHandlers.forEach((s, ch) -> ShutdownUtil.shutdown(LOGGER, ch::shutdown));
        clientHandlers.clear();
        ShutdownUtil.shutdown(LOGGER, clientTransport::shutdown);
    }

    private final class ClientHandler implements Runnable {
        private volatile boolean isRunning = true;
        private final ObjectIOClient ioClient;
        private final ExecutorService requestExecutor;

        private ClientHandler(ObjectIOClient ioClient, int handlerId) throws IOException {
            this.ioClient = ioClient;
            this.requestExecutor = Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder()
                            .namingPattern("server-" + serverId +"-handler-" + handlerId + "-exec-" + "%d")
                            .daemon(false)
                            .build());
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Message request = (Message) ioClient.readObject();
                    LOGGER.debug("IN (request): {}", request);
                    sampleReceive(request);

                    // we allow multiple requests from the same client, hence, we don't block on processing the
                    // request and writing the response back to the client.
                    requestExecutor.submit(() -> {
                        Message response = messageProcessor.apply(request);
                        try {
                            synchronized (ioClient) {
                                LOGGER.debug("OUT (response): {}", response);
                                ioClient.writeObject(response);
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
            ShutdownUtil.close(LOGGER, ioClient);
            ShutdownUtil.shutdown(LOGGER, requestExecutor);
        }
    }

    private static void sampleSend(Message message) {
        sample("insights.server.send.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sampleReceive(Message message) {
        sample("insights.server.receive.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sample(String name, Message message) {
        if (EventManagerService.getInstance().listenerExists(MeasurementEvent.class)) {
            // fire async measurement event
            EventManagerService.getInstance().fireAsync(
                    new MeasurementEvent(MeasurementEvent.MeasurementType.SAMPLE, name, message));
        }
    }
}
