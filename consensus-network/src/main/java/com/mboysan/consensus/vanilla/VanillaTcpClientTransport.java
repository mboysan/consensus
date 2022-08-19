package com.mboysan.consensus.vanilla;

import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.ThrowingRunnable;
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

    private final Map<Integer, Destination> destinations;
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
            throw new IOException(e);
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
            Destination dest = destinations.get(id);
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
        clientPools.forEach((i, pool) -> shutdown(pool::close));
        clientPools.clear();
    }

    private static void shutdown(ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        }
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

        TcpClient(int clientId, int associatedServerId, Destination destination) throws IOException {
            this.socket = new Socket(destination.ip(), destination.port());
            this.os = new ObjectOutputStream(socket.getOutputStream());
            this.is = new ObjectInputStream(socket.getInputStream());
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
                    Message response = (Message) is.readObject();
                    LOGGER.debug("IN (response): {}", response);
                    CompletableFuture<Message> future = callbackMap.remove(response.getId());
                    if (future != null) {
                        future.complete(response);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error(e.getMessage());
                    VanillaTcpClientTransport.shutdown(this::shutdown);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                    VanillaTcpClientTransport.shutdown(this::shutdown);
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
        }

        synchronized void shutdown() {
            if (!isConnected) {
                return;
            }
            isConnected = false;
            VanillaTcpClientTransport.shutdown(() -> {if (os != null) os.close();});
            VanillaTcpClientTransport.shutdown(() -> {if (is != null) is.close();});
            VanillaTcpClientTransport.shutdown(() -> {if (socket != null) socket.close();});
            semaphore.release();
        }
    }

    private class TcpClientFactory extends BasePooledObjectFactory<TcpClient> {
        private final AtomicInteger clientId = new AtomicInteger(0);
        private final Destination destination;

        private TcpClientFactory(Destination destination) {
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
