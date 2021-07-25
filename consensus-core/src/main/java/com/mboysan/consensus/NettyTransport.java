package com.mboysan.consensus;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NettyTransport implements Transport {

    private static final long DEFAULT_CALLBACK_TIMEOUT_MS = 5000;

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTransport.class);

    private final int port;
    private final Map<Integer, String> destinations;
    private volatile boolean isRunning = false;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private RPCProtocol requestProcessor;

    private final Map<Integer, ObjectPool<NettyClient>> clientPools = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();

    public NettyTransport(int port, Map<Integer, String> destinations) {
        this.port = port;
        this.destinations = destinations;
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public synchronized void start() throws IOException {
        if (isRunning) {
            return;
        }
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<Message>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
                                    recv(msg);
                                }
                            });
                        }
                    })
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            channel = b.bind(port).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        isRunning = true;
    }

    private void recv(Message message) {
        if (!isRunning) {
            throw new IllegalStateException("server is not running (1)");
        }
        CompletableFuture<Message> msgFuture = callbackMap.remove(message.getCorrelationId());
        if (msgFuture != null) {
            // this is a response
            LOGGER.debug("IN (response) : {}", message);
            msgFuture.complete(message);
        } else {
            // this is a request
            Message response = requestProcessor.apply(message);
            try {
                sendUsingClientPool(response);
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void addNode(int nodeId, RPCProtocol requestProcessor) {
        Objects.requireNonNull(destinations);
        Set<Integer> nodeIds = new HashSet<>();
        destinations.forEach((id, dest) -> {
            if (id != nodeId) { // we don't add clients for ourself
                NettyClientFactory clientFactory = new NettyClientFactory(dest);
                clientPools.put(id, new GenericObjectPool<>(clientFactory));
                nodeIds.add(id);
            }
        });
        this.requestProcessor = requestProcessor;
        requestProcessor.onNodeListChanged(nodeIds);
    }

    @Override
    public void removeNode(int nodeId) {
        // No need to implement this method for this transport at the moment.
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) {
        throw new UnsupportedOperationException("unsupported with this transport");
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        if (!isRunning) {
            throw new IllegalStateException("server is not running (2)");
        }
        if (message.getCorrelationId() == null) {
            throw new IllegalArgumentException("correlationId must not be null");
        }
        LOGGER.debug("OUT (sendRecv) : {}", message);
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getCorrelationId(), msgFuture);
        try {
            sendUsingClientPool(message);

            // fixme: possible memory leak due to msgFuture not being removed from callbackMap in case of Exception
            return msgFuture.get(DEFAULT_CALLBACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void sendUsingClientPool(Message message) throws IOException {
        ObjectPool<NettyClient> pool = clientPools.get(message.getReceiverId());
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

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        try {
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
        try {
            bossGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
        if (channel != null) {
            channel.close();
        }
        clientPools.forEach((i, pool) -> pool.close());
        callbackMap.forEach((s, f) -> f.cancel(true));
        callbackMap.clear();
    }

    private static class NettyClient {
        private final EventLoopGroup group;
        private SocketChannel channel;
        private final InetAddress ip;
        private final int port;

        public NettyClient(String destAddress) throws UnknownHostException {
            String[] dest = destAddress.split(":");
            this.ip = InetAddress.getByName(dest[0]);
            this.port = Integer.parseInt(dest[1]);
            this.group = new NioEventLoopGroup(1);
        }

        public synchronized void connect() throws IOException {
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addLast(new ObjectEncoder());
                            }
                        });
                ChannelFuture f = b.connect(ip, port).sync();
                this.channel = (SocketChannel) f.channel();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        public boolean isValid() {
            return channel != null && isConnected();
        }

        public boolean isConnected() {
            return channel.isActive();
        }

        public void send(Message message) {
            channel.writeAndFlush(message);
        }

        public synchronized void shutdown() {
            group.shutdownGracefully();
        }
    }

    private static class NettyClientFactory extends BasePooledObjectFactory<NettyClient> {
        private final String destAddress;

        private NettyClientFactory(String destAddress) {
            this.destAddress = destAddress;
        }

        @Override
        public NettyClient create() throws Exception {
            return new NettyClient(destAddress);
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
