package com.mboysan.consensus;

import com.mboysan.consensus.util.TimerQueue;
import com.mboysan.consensus.util.Timers;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    private final Timers timers;

    ExecutorService peerExecutor;
    ExecutorService commandExecutor;

    final Map<Integer, P> peers = new HashMap<>();

    AbstractNode(int nodeId, Transport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        this.timers = createTimers();
    }

    Timers createTimers() {
        return new TimerQueue();
    }

    @Override
    public synchronized Future<Void> start() throws IOException {
        if (isRunning) {
            return CompletableFuture.completedFuture(null); // ignore call to start
        }
        if (!transport.isShared()) {
            transport.start();
        }
        isRunning = true;

        transport.addNode(nodeId, this);
        peerExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new BasicThreadFactory.Builder().namingPattern("PeerExec-" + nodeId + "-%d").daemon(true).build()
        );
        commandExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new BasicThreadFactory.Builder().namingPattern("CmdExec-" + nodeId + "-%d").daemon(true).build()
        );

        return startNode();
    }

    abstract Future<Void> startNode();

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        timers.shutdown();
        commandExecutor.shutdown();
        peerExecutor.shutdown();
        peers.clear();
        transport.removeNode(nodeId);
        if (!transport.isShared()) {
            transport.shutdown();
        }
        shutdownNode();
    }

    abstract void shutdownNode();

    @Override
    public synchronized void onNodeListChanged(Set<Integer> serverIds) {
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

    Transport getTransport() {
        return transport;
    }

    Timers getTimers() {
        return timers;
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isRunning() {
        return isRunning;
    }
}