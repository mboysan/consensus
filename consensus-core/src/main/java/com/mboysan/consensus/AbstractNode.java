package com.mboysan.consensus;

import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.event.NodeListChangedEvent;
import com.mboysan.consensus.event.NodeStartedEvent;
import com.mboysan.consensus.event.NodeStoppedEvent;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.Scheduler;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

abstract class AbstractNode<P extends AbstractPeer> implements RPCProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNode.class);

    private volatile boolean isRunning;

    private final int nodeId;
    private final Transport transport;
    private final Scheduler scheduler;

    private ExecutorService peerExecutor;

    final Map<Integer, P> peers = new ConcurrentHashMap<>();

    private final NodeConfig nodeConfig;

    AbstractNode(NodeConfig config, Transport transport) {
        LOGGER.info("node-{} config={}", config.nodeId(), config);

        this.nodeId = config.nodeId();
        this.transport = transport;
        this.scheduler = new Scheduler();
        this.nodeConfig = config;

        EventManagerService.getInstance().register(NodeListChangedEvent.class, this::onNodeListChanged);
    }

    public synchronized Future<Void> start() throws IOException {
        if (isRunning) {
            return CompletableFuture.completedFuture(null); // ignore call to start
        }
        if (!transport.isShared()) {
            transport.start();
        }
        isRunning = true;

        // register known peer destinations
        onNodeListChanged(new NodeListChangedEvent(nodeId, transport.getDestinationNodeIds()));

        transport.registerMessageProcessor(this);

        peerExecutor = createPeerExecutorService();

        EventManagerService.getInstance().fire(new NodeStartedEvent(nodeId));

        return startNode();
    }

    private ExecutorService createPeerExecutorService() {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("node-" + nodeId + "-peer-exec-%d").daemon(true).build();
        final int numThreads = this.nodeConfig.nodePeerExecutorThreadCount();
        if (numThreads < 0) {
            LOGGER.info("peerExecutor thread pool size=unlimited");
            return Executors.newCachedThreadPool(threadFactory);
        } else if (numThreads == 0) {
            final int threadCount = Runtime.getRuntime().availableProcessors() * 2;
            LOGGER.info("peerExecutor thread pool size={}", threadCount);
            return Executors.newFixedThreadPool(threadCount, threadFactory);
        }
        LOGGER.info("peerExecutor thread pool size={}", numThreads);
        return Executors.newFixedThreadPool(numThreads, threadFactory);
    }

    abstract Future<Void> startNode();

    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        scheduler.shutdown();
        peerExecutor.shutdown();
        peers.clear();
        EventManagerService.getInstance().fire(new NodeStoppedEvent(nodeId));
        if (!transport.isShared()) {
            transport.shutdown();
        }
        shutdownNode();
        LOGGER.info("node-{} shutdown", getNodeId());
    }

    void shutdownNode() {
        // override if a special logic is needed.
    }

    public synchronized boolean isRunning() {
        return isRunning;
    }

    abstract void update();

    private synchronized void onNodeListChanged(NodeListChangedEvent event) {
        if (event.targetNodeId() != nodeId) {
            return;
        }
        Set<Integer> serverIds = event.serverIds();
        // first, we add new peers for each new serverId.
        serverIds.forEach(peerId -> peers.computeIfAbsent(peerId, this::createPeer));

        // next, we remove all peers who are not in the serverIds set.
        Set<Integer> difference = new HashSet<>(peers.keySet());
        difference.removeAll(serverIds);
        peers.keySet().removeAll(difference);
    }

    abstract P createPeer(int peerId);

    void forEachPeerParallel(Consumer<P> peerConsumer) {
        List<Future<?>> futures = new ArrayList<>();
        peers.forEach((id, peer) -> {
            if (id != nodeId) {
                futures.add(peerExecutor.submit(() -> peerConsumer.accept(peer)));
            } else {
                peerConsumer.accept(peer);
            }
        });
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    void validateAction() {
        if (!isRunning) {
            throw new IllegalStateException("node-" + nodeId + " not running");
        }
    }

    @SuppressWarnings("unchecked")
    <Q extends Message, S extends Message> S routeMessage(Q message, int receiverId) throws IOException {
        validateAction();
        LOGGER.debug("routing message={}, from={} to={}", message, getNodeId(), receiverId);
        message.setReceiverId(receiverId).setSenderId(getNodeId());
        return (S) transport.sendRecv(message);
    }

    abstract RPCProtocol getRPC();

    Scheduler getScheduler() {
        return scheduler;
    }

    public int getNodeId() {
        return nodeId;
    }

    NodeConfig getConfiguration() {
        return nodeConfig;
    }
}
