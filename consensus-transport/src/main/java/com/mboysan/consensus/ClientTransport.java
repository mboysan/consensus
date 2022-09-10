package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TransportConfig;
import com.mboysan.consensus.configuration.TransportConfigHelper;
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

class ClientTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTransport.class);

    private final Map<Integer, ? extends Destination> destinations;
    private final int clientPoolSize;
    private volatile boolean isRunning = false;

    private final long messageCallbackTimeoutMs;
    private final Map<Integer, ObjectPool<Client>> clientPools = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();
    private final TransportConfig config;
    private final int associatedServerId;
    private final FailureDetector failureDetector;

    ClientTransport(TransportConfig config) {
        this(config, -1);
    }

    ClientTransport(TransportConfig config, int associatedServerId) {
        LOGGER.info("client transport for server={} config={}", associatedServerId, config);
        this.config = Objects.requireNonNull(config);
        this.destinations = Objects.requireNonNull(TransportConfigHelper.resolveDestinations(config));
        this.messageCallbackTimeoutMs = config.messageCallbackTimeoutMs();
        this.clientPoolSize = resolveClientPoolSize(config.clientPoolSize());
        this.associatedServerId = associatedServerId;
        this.failureDetector = createFailureDetector();
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public synchronized void start() {
        isRunning = true;
    }

    @Override
    public void registerMessageProcessor(UnaryOperator<Message> messageProcessor) {
        throw new UnsupportedOperationException("registerMessageProcessor unsupported.");
    }

    FailureDetector createFailureDetector() {
        return new FailureDetector(this, config, associatedServerId);
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
        if (failureDetector != null) {
            failureDetector.validateStability(message.getReceiverId());
        }
        ObjectPool<Client> pool = getOrCreateClientPool(message.getReceiverId());
        Client client = null;
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getId(), msgFuture);
        try {
            client = pool.borrowObject();
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
            if (failureDetector != null) {
                failureDetector.markFailed(message.getReceiverId());
            }
            throw e;
        } finally {
            callbackMap.remove(message.getId());
        }
    }

    private ObjectPool<Client> getOrCreateClientPool(int receiverId) {
        return clientPools.computeIfAbsent(receiverId, id -> {
            Destination dest = destinations.get(id);
            ClientFactory clientFactory = new ClientFactory(dest);
            GenericObjectPoolConfig<Client> poolConfig = new GenericObjectPoolConfig<>();
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
        if (failureDetector != null) {
            failureDetector.shutdown();
        }
        clientPools.forEach((i, pool) -> ShutdownUtil.close(LOGGER, pool));
        clientPools.clear();
    }

    private static int resolveClientPoolSize(int providedClientPoolSize) {
        if (providedClientPoolSize == 0) {
            return Runtime.getRuntime().availableProcessors() * 2;
        }
        return providedClientPoolSize;
    }

    private final class Client {
        private volatile boolean isConnected;
        private final ObjectIOClient ioClient;
        private final Semaphore semaphore = new Semaphore(0);

        Client(int clientId, int associatedServerId, Destination destination) throws IOException {
            this.ioClient = ObjectIOClient.create(destination);
            this.isConnected = true;

            String receiverThreadName = associatedServerId == -1
                    ? "client-%d-recv-server-%d".formatted(clientId, destination.nodeId())
                    : "server-%d-client-%d-recv-server-%d".formatted(associatedServerId, clientId, destination.nodeId());
            Thread receiverThread = new Thread(this::receive, receiverThreadName);
            receiverThread.start();
        }

        void receive() {
            while (isConnected) {
                try {
                    semaphore.acquire();
                    Message response = (Message) ioClient.readObject();
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
            ioClient.writeObject(message);
            sampleSend(message);
        }

        synchronized void shutdown() {
            if (!isConnected) {
                return;
            }
            isConnected = false;
            ShutdownUtil.close(LOGGER, ioClient);
            semaphore.release();
        }
    }

    private static void sampleSend(Message message) {
        sample("insights.client.send.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sampleReceive(Message message) {
        sample("insights.client.receive.sizeOf." + message.getClass().getSimpleName(), message);
    }

    private static void sample(String name, Message message) {
        if (EventManagerService.getInstance().listenerExists(MeasurementEvent.class)) {
            // fire async measurement event
            EventManagerService.getInstance().fireAsync(
                    new MeasurementEvent(MeasurementEvent.MeasurementType.SAMPLE, name, message));
        }
    }

    private class ClientFactory extends BasePooledObjectFactory<Client> {
        private final AtomicInteger clientId = new AtomicInteger(0);
        private final Destination destination;

        private ClientFactory(Destination destination) {
            this.destination = destination;
        }

        @Override
        public Client create() throws IOException {
            return new Client(clientId.getAndIncrement(), associatedServerId, this.destination);
        }

        @Override
        public void destroyObject(PooledObject<Client> p) {
            p.getObject().shutdown();
        }

        @Override
        public PooledObject<Client> wrap(Client client) {
            return new DefaultPooledObject<>(client);
        }
    }
}
