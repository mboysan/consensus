package com.mboysan.consensus;

import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.CheckedSupplier;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;

public class NettyServerTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerTransport.class);

    private final int port;
    private final Map<Integer, String> destinations;
    private volatile boolean isRunning = false;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private RPCProtocol requestProcessor;

    private final NettyClientTransport clientTransport;

    public NettyServerTransport(NettyTransportConfig config) {
        this.port = config.port();
        this.destinations = config.destinations();
        this.clientTransport = new NettyClientTransport(config);
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
        try {
            ServerBootstrap b = new ServerBootstrap();
            this.bossGroup = new NioEventLoopGroup();
            this.workerGroup = new NioEventLoopGroup();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
                            ch.pipeline().addLast(new ObjectEncoder());

                            ch.pipeline().addLast(new SimpleChannelInboundHandler<Message>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, Message request) throws IOException {
                                    LOGGER.debug("IN (request): {}", request);
                                    Message response = requestProcessor.processRequest(request);
                                    ctx.writeAndFlush(response);
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
        clientTransport.start();
        isRunning = true;
    }

    @Override
    public void addNode(int nodeId, RPCProtocol requestProcessor) {
        Objects.requireNonNull(destinations);
        Set<Integer> nodeIds = new HashSet<>();
        destinations.forEach((id, dest) -> {
            if (id != nodeId) { // we don't add ourselves
                nodeIds.add(id);
            }
        });
        this.requestProcessor = requestProcessor;
        requestProcessor.onNodeListChanged(nodeIds);
    }

    @Override
    public void removeNode(int nodeId) {
        clientTransport.removeNode(nodeId);
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) {
        if (!isRunning) {
            throw new IllegalStateException("server is not running (2)");
        }
        return clientTransport.sendRecvAsync(message);
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        if (!isRunning) {
            throw new IllegalStateException("server is not running (2)");
        }
        return clientTransport.sendRecv(message);
    }

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        shutdown(() -> workerGroup.shutdownGracefully().sync());
        shutdown(() -> bossGroup.shutdownGracefully().sync());
        if (channel != null && channel.isOpen()) {
            shutdown(() -> channel.close().sync());
        }
        clientTransport.shutdown();
    }

    private void shutdown(CheckedSupplier<?> toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).get();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public synchronized boolean verifyShutdown() {
        return !isRunning
                && workerGroup.isTerminated()
                && bossGroup.isTerminated()
                && (channel == null || !channel.isOpen())
                && clientTransport.verifyShutdown();
    }
}
