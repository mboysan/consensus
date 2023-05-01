package com.mboysan.consensus.vanilla;

import com.mboysan.consensus.EventManagerService;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.TcpDestination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.ShutdownUtil;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

public class VanillaTcpClientTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpClientTransport.class);

    private final Map<Integer, TcpDestination> destinations;
    private final int clientPoolSize;
    private volatile boolean isRunning = false;

    private final long messageCallbackTimeoutMs;
    private final Map<Integer, ObjectPool<TcpClient>> clientPools = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();
    private final int associatedServerId;

    private final FailureDetector failureDetector;

    public VanillaTcpClientTransport(TcpTransportConfig config) {
        this(config, -1);
    }

    VanillaTcpClientTransport(TcpTransportConfig config, int associatedServerId) {
        LOGGER.info("client transport for server={} config={}", associatedServerId, config);

        this.destinations = Objects.requireNonNull(config.destinations());
        this.messageCallbackTimeoutMs = config.messageCallbackTimeoutMs();
        this.clientPoolSize = resolveClientPoolSize(config.clientPoolSize());
        this.associatedServerId = associatedServerId;
        this.failureDetector = new FailureDetector(this, config, associatedServerId);
    }

    @Override
    public synchronized void start() {
        isRunning = true;
    }

    @Override
    public void registerMessageProcessor(UnaryOperator<Message> messageProcessor) {
        throw new UnsupportedOperationException("registerMessageProcessor unsupported.");
    }

    @Override
    public Set<Integer> getDestinationNodeIds() {
        return Collections.unmodifiableSet(destinations.keySet());
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) {
        throw new UnsupportedOperationException("sendRecvAsync unsupported.");
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        if (!isRunning) {
            throw new IllegalStateException("client is not running (2)");
        }
        if (message.getId() == null) {
            throw new IllegalArgumentException("msg id must not be null");
        }
        try {
            return sendRecvUsingClientPool(message);
        } catch (Exception e) {
            throw new IOException("I/O err for messageId=" + message.getId(), e);
        }
    }

    private Message sendRecvUsingClientPool(Message message) throws Exception {
        failureDetector.validateStability(message.getReceiverId());
        ObjectPool<TcpClient> pool = getOrCreateClientPool(message.getReceiverId());
        TcpClient client = null;
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getId(), msgFuture);
        try {
            client = pool.borrowObject();
            LOGGER.trace("TCPClients active={}, idle={}", pool.getNumActive(), pool.getNumIdle());
            client.send(message);
            Message response = messageCallbackTimeoutMs > 0
                    ? msgFuture.get(messageCallbackTimeoutMs, TimeUnit.MILLISECONDS)
                    : msgFuture.get();  // wait indefinitely
            pool.returnObject(client);
            return response;
        } catch (Exception e) {
            if (client != null) {
                pool.invalidateObject(client);
            }
            failureDetector.markFailed(message.getReceiverId());
            throw e;
        } finally {
            callbackMap.remove(message.getId());
        }
    }

    private ObjectPool<TcpClient> getOrCreateClientPool(int receiverId) {
        return clientPools.computeIfAbsent(receiverId, id -> {
            TcpDestination dest = destinations.get(id);
            TcpClientFactory clientFactory = new TcpClientFactory(dest);
            GenericObjectPoolConfig<TcpClient> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(clientPoolSize);
            return new GenericObjectPool<>(clientFactory, poolConfig);
        });
    }

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        failureDetector.shutdown();
        clientPools.forEach((i, pool) -> ShutdownUtil.close(LOGGER, pool));
        clientPools.clear();
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning && clientPools.size() == 0;
    }

    private static int resolveClientPoolSize(int providedClientPoolSize) {
        if (providedClientPoolSize == 0) {
            return Runtime.getRuntime().availableProcessors() * 2;
        }
        return providedClientPoolSize;
    }

    private final class TcpClient {
        private volatile boolean isConnected;
        private final Socket socket;
        private final ObjectOutputStream os;
        private final ObjectInputStream is;
        private final Semaphore semaphore = new Semaphore(0);

        TcpClient(int clientId, int associatedServerId, TcpDestination destination) throws IOException {
            this.socket = new Socket(destination.ip(), destination.port());
            this.os = new ObjectOutputStream(socket.getOutputStream());
            this.is = new ObjectInputStream(socket.getInputStream());
            this.isConnected = true;

            String receiverThreadName = associatedServerId == -1
                    ? "client-%d-recv-server-%d".formatted(clientId, destination.nodeId())
                    : "server-%d-client-%d-recv-server-%d".formatted(associatedServerId, clientId, destination.nodeId());
            Thread receiverThread = new Thread(this::receive, receiverThreadName);
            receiverThread.start();
            LOGGER.debug("created new TcpClient={}", receiverThreadName);
        }

        void receive() {
            while (isConnected) {
                try {
                    semaphore.acquire();
                    Message response = (Message) is.readObject();
                    LOGGER.debug("IN (response): {}", response);
                    sampleReceive(response);
                    CompletableFuture<Message> future = callbackMap.remove(response.getId());
                    if (future != null) {
                        future.complete(response);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error(e.getMessage());
                    shutdown();
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                    shutdown();
                    Thread.currentThread().interrupt();
                }
            }
        }

        synchronized void send(Message message) throws IOException {
            LOGGER.debug("OUT (request): {}", message);
            semaphore.release();
            os.writeObject(message);
            os.flush();
            os.reset();
            sampleSend(message);
        }

        synchronized void shutdown() {
            if (!isConnected) {
                return;
            }
            isConnected = false;
            ShutdownUtil.close(LOGGER, os);
            ShutdownUtil.close(LOGGER, is);
            ShutdownUtil.close(LOGGER, socket);
            semaphore.release();
        }
    }

    private static void sampleSend(Message message) {
        sample("insights.tcp.client.send.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sampleReceive(Message message) {
        sample("insights.tcp.client.receive.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sample(String name, Message message) {
        if (EventManagerService.getInstance().listenerExists(MeasurementEvent.class)) {
            // fire async measurement event
            EventManagerService.getInstance().fireAsync(
                    new MeasurementEvent(MeasurementEvent.MeasurementType.SAMPLE, name, message));
        }
    }

    private class TcpClientFactory extends BasePooledObjectFactory<TcpClient> {
        private final AtomicInteger clientId = new AtomicInteger(0);
        private final TcpDestination destination;

        private TcpClientFactory(TcpDestination destination) {
            this.destination = destination;
        }

        @Override
        public TcpClient create() throws IOException {
            return new TcpClient(clientId.getAndIncrement(), associatedServerId, this.destination);
        }

        @Override
        public void destroyObject(PooledObject<TcpClient> p) {
            p.getObject().shutdown();
        }

        @Override
        public PooledObject<TcpClient> wrap(TcpClient tcpClient) {
            return new DefaultPooledObject<>(tcpClient);
        }
    }
}
