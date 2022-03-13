package com.mboysan.consensus.vanilla;

import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.CheckedRunnable;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
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
import java.util.function.UnaryOperator;

public class VanillaTcpClientTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpClientTransport.class);

    private final Map<Integer, Destination> destinations;
    private final int clientPoolSize;
    private volatile boolean isRunning = false;

    private final long messageCallbackTimeoutMs;
    private final Map<Integer, ObjectPool<TcpClient>> clientPools = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();

    public VanillaTcpClientTransport(TcpTransportConfig config) {
        this.destinations = Objects.requireNonNull(config.destinations());
        this.messageCallbackTimeoutMs = config.messageCallbackTimeoutMs();
        this.clientPoolSize = resolveClientPoolSize(config.clientPoolSize());
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
            throw new IOException("err on message=" + message, e);
        }
    }

    private Message sendRecvUsingClientPool(Message message) throws Exception {
        ObjectPool<TcpClient> pool = getOrCreateClientPool(message.getReceiverId());
        TcpClient client = null;
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getId(), msgFuture);
        try {
            client = pool.borrowObject();
            client.send(message);
            return msgFuture.get(messageCallbackTimeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            if (client != null) {
                try {
                    pool.returnObject(client);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }
            callbackMap.remove(message.getId());
        }
    }

    private ObjectPool<TcpClient> getOrCreateClientPool(int receiverId) {
        return clientPools.computeIfAbsent(receiverId, id -> {
            Destination dest = destinations.get(id);
            TcpClientFactory clientFactory = new TcpClientFactory(dest, callbackMap);
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
        clientPools.forEach((i, pool) -> shutdown(pool::close));
        clientPools.clear();
    }

    private static void shutdown(CheckedRunnable<Exception> toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning && clientPools.size() == 0;
    }

    private static int resolveClientPoolSize(int providedClientPoolSize) {
        if (providedClientPoolSize <= 0) {
            return Runtime.getRuntime().availableProcessors() * 2;
        }
        return providedClientPoolSize;
    }

    private static class TcpClient {
        private volatile boolean isConnected;
        private final Socket socket;
        private final ObjectOutputStream os;
        private final ObjectInputStream is;
        private final Map<String, CompletableFuture<Message>> callbackMap;
        private final Semaphore semaphore = new Semaphore(0);

        TcpClient(
                Destination destination,
                Map<String, CompletableFuture<Message>> callbackMap) throws IOException
        {
            this.callbackMap = callbackMap;
            this.socket = new Socket(destination.ip(), destination.port());
            this.os = new ObjectOutputStream(socket.getOutputStream());
            this.is = new ObjectInputStream(socket.getInputStream());
            this.isConnected = true;

            String receiverThreadName = "client-receiver-for-" + destination.nodeId();
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
                } catch (EOFException ignore) {
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error(e.getMessage(), e);
                    VanillaTcpClientTransport.shutdown(this::shutdown);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
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
        }

        synchronized boolean isValid() {
            return socket != null && os != null && is != null && isConnected;
        }

        synchronized void shutdown() throws IOException {
            isConnected = false;
            if (os != null) {
                os.close();
            }
            if (is != null) {
                is.close();
            }
            if (socket != null) {
                socket.close();
            }
            semaphore.release();
        }
    }

    private static class TcpClientFactory extends BasePooledObjectFactory<TcpClient> {
        private final Destination destination;
        private final Map<String, CompletableFuture<Message>> callbackMap;

        private TcpClientFactory(
                Destination destination,
                Map<String, CompletableFuture<Message>> callbackMap)
        {
            this.destination = destination;
            this.callbackMap = callbackMap;
        }

        @Override
        public TcpClient create() throws IOException {
            return new TcpClient(destination, callbackMap);
        }

        @Override
        public void destroyObject(PooledObject<TcpClient> p) throws IOException {
            p.getObject().shutdown();
        }

        @Override
        public boolean validateObject(PooledObject<TcpClient> p) {
            return p.getObject().isValid();
        }

        @Override
        public PooledObject<TcpClient> wrap(TcpClient tcpClient) {
            return new DefaultPooledObject<>(tcpClient);
        }
    }
}
