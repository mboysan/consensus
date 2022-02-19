package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.message.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
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
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

public class NettyClientTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientTransport.class);

    private final Map<Integer, Destination> destinations;
    private final long messageCallbackTimeoutMs;
    private final int clientPoolSize;
    private volatile boolean isRunning = false;

    private final Map<Integer, ObjectPool<NettyClient>> clientPools = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();

    public NettyClientTransport(NettyTransportConfig config) {
        this.destinations = Objects.requireNonNull(config.destinations());
        this.messageCallbackTimeoutMs = config.messageCallbackTimeoutMs();
        this.clientPoolSize = config.clientPoolSize();
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
    public Set<Integer> getDestinationNodeIds() {
        return Collections.unmodifiableSet(destinations.keySet());
    }

    @Override
    public void registerMessageProcessor(UnaryOperator<Message> messageProcessor) {
        throw new UnsupportedOperationException("registerMessageProcessor unsupported.");
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
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getId(), msgFuture);
        try {
            sendUsingClientPool(message);
            return msgFuture.get(messageCallbackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            callbackMap.remove(message.getId());
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            callbackMap.remove(message.getId());
            throw new IOException(e);
        }
    }

    private void sendUsingClientPool(Message message) throws IOException {
        ObjectPool<NettyClient> pool = getOrCreateClientPool(message.getReceiverId());
        NettyClient client = null;
        try {
            client = pool.borrowObject();
            client.send(message);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (client != null) {
                try {
                    pool.returnObject(client);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
    }

    private ObjectPool<NettyClient> getOrCreateClientPool(int receiverId) {
        return clientPools.computeIfAbsent(receiverId, id -> {
            Destination dest = destinations.get(id);
            NettyClientFactory clientFactory = new NettyClientFactory(dest, callbackMap);
            GenericObjectPoolConfig<NettyClient> poolConfig = new GenericObjectPoolConfig<>();
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
        clientPools.forEach((i, pool) -> pool.close());
        clientPools.clear();
        callbackMap.forEach((s, f) -> f.cancel(true));
        callbackMap.clear();
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning && callbackMap.size() == 0 && clientPools.size() == 0;
    }

    private static class NettyClient {
        private final EventLoopGroup group;
        private SocketChannel channel;
        private final Destination destination;
        private final Map<String, CompletableFuture<Message>> callbackMap;

        NettyClient(Destination destination, Map<String, CompletableFuture<Message>> callbackMap) {
            this.destination = destination;
            this.group = new NioEventLoopGroup(1);
            this.callbackMap = callbackMap;
        }

        synchronized void connect() throws IOException {
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
                                ch.pipeline().addLast(new ObjectEncoder());

                                ch.pipeline().addLast(new SimpleChannelInboundHandler<Message>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Message response) {
                                        CompletableFuture<Message> msgFuture = callbackMap.remove(response.getId());
                                        if (msgFuture != null) {
                                            msgFuture.complete(response);
                                        }
                                    }
                                });
                            }
                        });
                ChannelFuture f = b.connect(destination.ip(), destination.port()).sync();
                this.channel = (SocketChannel) f.channel();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        boolean isValid() {
            return channel != null && isConnected();
        }

        boolean isConnected() {
            return channel.isActive();
        }

        void send(Message message) {
            LOGGER.debug("OUT (request): {}", message);
            channel.writeAndFlush(message);
        }

        synchronized void shutdown() {
            group.shutdownGracefully();
        }
    }

    private static class NettyClientFactory extends BasePooledObjectFactory<NettyClient> {
        private final Destination destination;
        private final Map<String, CompletableFuture<Message>> callbackMap;

        private NettyClientFactory(Destination destination, Map<String, CompletableFuture<Message>> callbackMap) {
            this.destination = destination;
            this.callbackMap = callbackMap;
        }

        @Override
        public NettyClient create() {
            return new NettyClient(destination, callbackMap);
        }

        @Override
        public void activateObject(PooledObject<NettyClient> p) throws IOException {
            p.getObject().connect();
        }

        @Override
        public void destroyObject(PooledObject<NettyClient> p) {
            p.getObject().shutdown();
        }

        @Override
        public boolean validateObject(PooledObject<NettyClient> p) {
            return p.getObject().isValid();
        }

        @Override
        public PooledObject<NettyClient> wrap(NettyClient nettyClient) {
            return new DefaultPooledObject<>(nettyClient);
        }
    }
}
