package com.mboysan.consensus.vanilla;

import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.CheckedRunnable;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

public class VanillaTcpServerTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpServerTransport.class);

    private final int port;
    private final Map<Integer, Destination> destinations;
    private final int nodeId;
    private final Map<String, ClientHandler> clientHandlers = new ConcurrentHashMap<>();

    private ServerSocket serverSocket;
    private ExecutorService clientHandlerExecutor;

    private volatile boolean isRunning = false;
    private UnaryOperator<Message> messageProcessor;

    /**
     * Id of the node that this transport is responsible from
     */
    private final VanillaTcpClientTransport clientTransport;

    public VanillaTcpServerTransport(TcpTransportConfig config) {
        this.port = config.port();
        this.destinations = config.destinations();
        this.clientTransport = new VanillaTcpClientTransport(config);
        this.nodeId = destinations.values().stream()
                .filter(dest -> dest.port() == port)
                .mapToInt(Destination::nodeId)
                .findFirst().orElse(port);
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
    public synchronized void start() throws IOException {
        if (isRunning) {
            return;
        }
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Thread serverThread = new Thread(this::serverThreadLoop, "server-" + nodeId);
        serverThread.setDaemon(false);
        this.clientHandlerExecutor = Executors.newCachedThreadPool(
                new BasicThreadFactory.Builder()
                        .namingPattern("server-" + nodeId +"-handler-" + "%d")
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
                ClientHandler clientHandler = new ClientHandler(clientSocket, messageProcessor, clientCount, nodeId);
                clientHandlers.put(clientCount + "", clientHandler);
                clientHandlerExecutor.submit(clientHandler);
                ++clientCount;
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
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
        return clientTransport.sendRecv(message);
    }

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        shutdown(clientTransport::shutdown);
        clientHandlers.forEach((s, ch) -> shutdown(ch::shutdown));
        clientHandlers.clear();
        shutdown(clientHandlerExecutor::shutdownNow);
        shutdown(serverSocket::close);
    }

    private void shutdown(CheckedRunnable<Exception> toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning && clientTransport.verifyShutdown();
    }

    private static final class ClientHandler implements Runnable {

        private volatile boolean isRunning = true;
        private final Socket socket;
        private final ObjectOutputStream os;
        private final ObjectInputStream is;
        private final UnaryOperator<Message> messageProcessor;
        private final int handlerId;
        private final int nodeId;

        private ExecutorService requestExecutor;

        private ClientHandler(
                Socket socket,
                UnaryOperator<Message> messageProcessor,
                int handlerId,
                int nodeId) throws IOException
        {
            this.socket = socket;
            this.os = new ObjectOutputStream(socket.getOutputStream());
            this.is = new ObjectInputStream(socket.getInputStream());
            this.messageProcessor = messageProcessor;
            this.handlerId = handlerId;
            this.nodeId = nodeId;
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Message request = (Message) is.readObject();
                    LOGGER.debug("IN (request): {}", request);

                    // we allow multiple requests from the same client, hence, we don't block on processing the
                    // request and writing the response back to the client.
                    execute(() -> {
                        Message response = messageProcessor.apply(request);
                        try {
                            synchronized (os) {
                                LOGGER.debug("OUT (response): {}", response);
                                os.writeObject(response);
                                os.flush();
                            }
                        } catch (IOException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    });
                } catch (EOFException ignore) {
                } catch (Exception e) {
                    LOGGER.error("request could not be processed, err={}", e.getMessage());
                }
            }
        }

        synchronized void execute(Runnable runnable) {
            if (this.requestExecutor == null) {
                this.requestExecutor = Executors.newCachedThreadPool(
                        new BasicThreadFactory.Builder()
                                .namingPattern("server-" + nodeId +"-handler-" + handlerId + "-exec-" + "%d")
                                .daemon(false)
                                .build());
            }
            requestExecutor.submit(runnable);
        }

        synchronized void shutdown() throws IOException {
            if (requestExecutor != null) {
                requestExecutor.shutdownNow();
            }
            if (socket != null) {
                socket.close();
            }
            if (os != null) {
                synchronized (os) {
                    os.close();
                }
            }
            if (is != null) {
                is.close();
            }
            isRunning = false;
        }
    }
}
