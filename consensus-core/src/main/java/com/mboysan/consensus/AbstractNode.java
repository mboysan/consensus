package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.event.NodeListChangedEvent;
import com.mboysan.consensus.event.NodeStartedEvent;
import com.mboysan.consensus.event.NodeStoppedEvent;
import com.mboysan.consensus.util.TimerQueue;
import com.mboysan.consensus.util.Timers;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

abstract class AbstractNode<P extends AbstractPeer> implements RPCProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNode.class);

    private volatile boolean isRunning;
    private final Lock updateLock = new ReentrantLock();

    private final int nodeId;
    private final Transport transport;
    private final Timers timers;

    private ExecutorService peerExecutor;

    final Map<Integer, P> peers = new ConcurrentHashMap<>();

    private final Configuration nodeConfig;

    AbstractNode(Configuration config, Transport transport) {
        this.nodeId = config.nodeId();
        this.transport = transport;
        this.timers = createTimers();
        this.nodeConfig = config;
        LOGGER.info("node-{} config={}", nodeId, nodeConfig);

        EventManager.getInstance().registerEventListener(NodeListChangedEvent.class, this::onNodeListChanged);
    }

    Timers createTimers() {
        return new TimerQueue();
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

        peerExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new BasicThreadFactory.Builder().namingPattern("PeerExec-" + nodeId + "-%d").daemon(true).build()
        );

        EventManager.getInstance().fireEvent(new NodeStartedEvent(nodeId));

        return startNode();
    }

    abstract Future<Void> startNode();

    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        timers.shutdown();
        peerExecutor.shutdown();
        peers.clear();
        EventManager.getInstance().fireEvent(new NodeStoppedEvent(nodeId));
        if (!transport.isShared()) {
            transport.shutdown();
        }
        shutdownNode();
    }

    void shutdownNode() {
        // override if a special logic is needed.
    }

    void tryUpdate() {
        if (updateLock.tryLock()) {
            try {
                LOGGER.debug("node-{} update timeout, time={}", getNodeId(), getTimers().currentTime());
                update();
            } finally {
                updateLock.unlock();
            }
        } else {
            LOGGER.debug("update in progress, skipped.");
        }
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
            throw new IllegalStateException("raft node-" + nodeId + " not running");
        }
    }

    abstract RPCProtocol getRPC();

    Timers getTimers() {
        return timers;
    }

    public int getNodeId() {
        return nodeId;
    }

    Configuration getConfiguration() {
        return nodeConfig;
    }
}
