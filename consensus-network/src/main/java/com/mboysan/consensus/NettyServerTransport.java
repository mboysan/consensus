package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.CheckedSupplier;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

public class NettyServerTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerTransport.class);

    private final int port;
    private final Map<Integer, Destination> destinations;
    private volatile boolean isRunning = false;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    /**
     * Id of the node that this transport is responsible from
     */
    private final NettyClientTransport clientTransport;
    private UnaryOperator<Message> messageProcessor;

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
                                protected void channelRead0(ChannelHandlerContext ctx, Message request) {
                                    try {
                                        LOGGER.debug("IN (request): {}", request);
                                        Message response = messageProcessor.apply(request);
                                        LOGGER.debug("OUT (response): {}", response);
                                        ctx.writeAndFlush(response);
                                    } catch (Exception e) {
                                        LOGGER.error("request could not be processed, err={}", e.getMessage());
                                        throw e;
                                    }
                                }
                            });
                        }
                    })
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            channel = b.bind(port).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
        clientTransport.start();
        isRunning = true;
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
        shutdown(() -> workerGroup.shutdownGracefully().sync());
        shutdown(() -> bossGroup.shutdownGracefully().sync());
        if (channel != null && channel.isOpen()) {
            shutdown(() -> channel.close().sync());
        }
        clientTransport.shutdown();
    }

    private void shutdown(CheckedSupplier<?, Exception> toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).get();
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
