package com.mboysan.dist;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class InVMTransport implements Transport {

    private static final long DEFAULT_CALLBACK_TIMEOUT_MS = 5000;

    private static final Logger LOGGER = LoggerFactory.getLogger(InVMTransport.class);

    private final ExecutorService serverExecutor = Executors.newCachedThreadPool(
            new BasicThreadFactory.Builder().namingPattern("ServerExec-%d").daemon(true).build()
    );

    private final Map<Integer, Server> serverMap = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();

    @Override
    public synchronized void addServer(int nodeId, RPCProtocol protoServer) {
        Server server = serverMap.get(nodeId);
        if (server == null) {
            server = new Server(protoServer);
            // add this server to map and start processing
            serverMap.put(nodeId, server);
            serverMap.forEach((i, s) -> s.protoServer.onServerListChanged(Set.copyOf(serverMap.keySet())));
            serverExecutor.execute(server);
        }
        LOGGER.info("server-{} added", nodeId);
    }

    @Override
    public synchronized void removeServer(int nodeId) {
        Server server = serverMap.get(nodeId);
        if (server != null) {
            Set<Integer> idsTmp = new HashSet<>(serverMap.keySet());
            idsTmp.remove(nodeId);
            serverMap.forEach((i, s) -> s.protoServer.onServerListChanged(Set.copyOf(idsTmp)));
            serverMap.remove(nodeId);
        }
        LOGGER.info("server-{} removed", nodeId);
    }

    public synchronized void connectedToNetwork(int nodeId, boolean isConnected) {
        serverMap.get(nodeId).isConnectedToNetwork = isConnected;
        LOGGER.info("server-{} network connected={} ===================", nodeId, isConnected);
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        verifySenderAlive(message);
        verifyReceiverAlive(message);

        LOGGER.debug("OUT (sendRecv) : {}", message);
        if (message.getSenderId() == message.getReceiverId()) {
            return sendRecvSelf(message);
        }
        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getCorrelationId(), msgFuture);
        serverMap.get(message.getReceiverId()).add(message);
        try {
            return msgFuture.get(DEFAULT_CALLBACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) throws IOException {
        verifySenderAlive(message);
        verifyReceiverAlive(message);

        if (message.getSenderId() == message.getReceiverId()) {
            return CompletableFuture.completedFuture(sendRecvSelf(message));
        }

        CompletableFuture<Message> msgFuture = new CompletableFuture<>();
        callbackMap.put(message.getCorrelationId(), msgFuture);
        serverMap.get(message.getReceiverId()).add(message);
        return msgFuture;
    }

    private Message sendRecvSelf(Message message) {
        if (message.getSenderId() != message.getReceiverId()) {
            throw new IllegalArgumentException("sender is not the receiver");
        }
        // this is the local server, no need to create a separate thread/task, so we do the processing
        // on the current thread.
        Message resp = serverMap.get(message.getReceiverId()).protoServer.apply(message);
        LOGGER.debug("IN (self) : {}", resp);
        return resp;
    }

    private void verifySenderAlive(Message message) throws IOException {
        if (!serverMap.get(message.getSenderId()).isConnectedToNetwork) {
            throw new IOException("sender is down, cannot send msg=" + message);
        }
    }

    private void verifyReceiverAlive(Message message) throws IOException {
        if (!serverMap.get(message.getReceiverId()).isConnectedToNetwork) {
            throw new IOException("receiver is down, cannot receive msg=" + message);
        }
    }

    public synchronized void shutdown() {
        serverExecutor.shutdown();
        callbackMap.clear();
        serverMap.forEach((i, server) -> server.shutdown());
        serverMap.clear();
    }


    class Server implements Runnable {
        volatile boolean isConnectedToNetwork = true;
        volatile boolean isRunning = true;
        final BlockingDeque<Message> messageQueue = new LinkedBlockingDeque<>();
        final RPCProtocol protoServer;

        Server(RPCProtocol protoServer) {
            this.protoServer = protoServer;
        }

        private void add(Message msg) throws IOException {
            if (!isRunning) {
                Future<Message> msgFuture = callbackMap.remove(msg.getCorrelationId());
                if (msgFuture != null) {
                    msgFuture.cancel(true);
                }
                throw new IOException("server is closed, cannot accept new messages, msg=" + msg);
            }
            messageQueue.offer(msg);
        }

        @Override
        public void run() {
            isRunning = true;
            while (isRunning) {
                try {
                    Message message = messageQueue.take();
                    LOGGER.debug("IN (req) : {}", message);
                    String correlationId = message.getCorrelationId();
                    if (correlationId == null) {
                        LOGGER.error("correlationId must not be null");
                        continue;
                    }
                    if (!isRunning || correlationId.equals("closingServer")) {
                        messageQueue.clear();
                        break;
                    }

                    // we first process the message
                    Message response = protoServer.apply(message);

                    // we send the response to the callback
                    CompletableFuture<Message> msgFuture = callbackMap.remove(message.getCorrelationId());
                    if (msgFuture != null) {
                        LOGGER.debug("OUT (resp) : {}", response);
                        msgFuture.complete(response);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error(e.getMessage(), e);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

        public synchronized void shutdown() {
            isConnectedToNetwork = false;
            isRunning = false;
            messageQueue.offer(new Message(){}.setCorrelationId("closingServer"));
        }
    }
}
