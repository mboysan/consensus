package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.InVMTransportConfig;
import com.mboysan.consensus.event.NodeListChangedEvent;
import com.mboysan.consensus.event.NodeStoppedEvent;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.SerializationUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class InVMTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(InVMTransport.class);

    private static final InVMTransportConfig DEFAULT_CONFIG;
    static {
        final Properties properties = new Properties();
        properties.put("transport.message.callbackTimeoutMs", 200 + "");    // set a default on callback timeout
        DEFAULT_CONFIG = CoreConfig.newInstance(InVMTransportConfig.class, properties);
    }

    private final ExecutorService serverExecutor = Executors.newCachedThreadPool(
            new BasicThreadFactory.Builder().namingPattern("invm-exec-%d").daemon(true).build()
    );

    private final Map<Integer, Server> serverMap = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Message>> callbackMap = new ConcurrentHashMap<>();

    private final InVMTransportConfig transportConfig;
    private final int associatedNodeId;

    public InVMTransport() {
        this(-1);
    }

    public InVMTransport(int associatedNodeId) {
        this(DEFAULT_CONFIG, associatedNodeId);
    }

    public InVMTransport(InVMTransportConfig transportConfig, int associatedNodeId) {
        EventManagerService.getInstance().register(NodeStoppedEvent.class, this::onNodeStopped);
        this.transportConfig = transportConfig;
        this.associatedNodeId = associatedNodeId;
    }

    @Override
    public void start() {
    }

    /**
     * A single instance of this type of transport will be shared among all the server nodes.
     *
     * @return true
     */
    @Override
    public boolean isShared() {
        return true;
    }

    @Override
    public synchronized void registerMessageProcessor(UnaryOperator<Message> messageProcessor) {
        if (messageProcessor instanceof AbstractNode<?> msgProcessor) {
            int nodeId = msgProcessor.getNodeId();
            Server server = serverMap.get(nodeId);
            if (server == null) {
                server = new Server(messageProcessor);
                // add this server to map and start processing
                serverMap.put(nodeId, server);
                serverMap.forEach((i, s) -> EventManagerService.getInstance().fire(
                        new NodeListChangedEvent(i, Set.copyOf(serverMap.keySet()))));
                serverExecutor.execute(server);
            }
            LOGGER.info("server-{} added", nodeId);
        } else {
            Server server = new Server(messageProcessor);
            serverMap.put(associatedNodeId, server);
            serverExecutor.execute(server);
            LOGGER.info("messageProcessor added");
        }
    }

    private synchronized void onNodeStopped(NodeStoppedEvent event) {
        int nodeId = event.sourceNodeId();
        Server server = serverMap.get(nodeId);
        if (server != null) {
            Set<Integer> idsTmp = new HashSet<>(serverMap.keySet());
            idsTmp.remove(nodeId);
            server.shutdown();
            serverMap.remove(nodeId);
            EventManagerService.getInstance().fire(new NodeListChangedEvent(nodeId, Set.copyOf(idsTmp)));
        }
        LOGGER.info("server-{} removed", nodeId);
    }

    @Override
    public Set<Integer> getDestinationNodeIds() {
        return Collections.unmodifiableSet(serverMap.keySet());
    }

    public synchronized void connectedToNetwork(int nodeId, boolean isConnected) {
        serverMap.get(nodeId).isConnectedToNetwork = isConnected;
        LOGGER.info("server-{} connected to network={} ===================", nodeId, isConnected);
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        Future<Message> msgFuture = sendRecvAsync(message);
        try {
            final long callbackTimeoutMs = transportConfig.messageCallbackTimeoutMs();
            Message response = callbackTimeoutMs > 0
                    ? msgFuture.get(callbackTimeoutMs, TimeUnit.MILLISECONDS)
                    : msgFuture.get();  // wait indefinitely.
            LOGGER.debug("IN (response): {}", response);
            return response;
        } catch (Exception e) {
            LOGGER.error("sendRecv failed for message={}", message, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException(e);
        } finally {
            callbackMap.remove(message.getId());
        }
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) throws IOException {
        verifySenderAlive(message);
        verifyReceiverAlive(message);

        try {
            LOGGER.debug("OUT (request) : {}", message);
            if (message.getSenderId() == message.getReceiverId()) {
                return CompletableFuture.completedFuture(sendRecvSelf(message));
            }
            CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            callbackMap.put(message.getId(), msgFuture);
            Server receivingServer = serverMap.get(message.getReceiverId());
            if (receivingServer == null) {
                throw new IOException("server-" + message.getReceiverId() + " is dead.");
            }
            receivingServer.add(message);
            return msgFuture;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Message sendRecvSelf(Message message) throws IOException {
        // this is the local server, no need to create a separate thread/task, so we do the processing
        // on the current thread.
        try {
            return serverMap.get(message.getReceiverId()).messageProcessor.apply(message);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void verifySenderAlive(Message message) throws IOException {
        if (message.getSenderId() == -1) {
            // sender is a client, we won't check client liveliness.
            return;
        }
        Server sendingServer = serverMap.get(message.getSenderId());
        if (sendingServer == null || !sendingServer.isConnectedToNetwork) {
            throw new IOException("sender is down, cannot send msg=" + message);
        }
    }

    private void verifyReceiverAlive(Message message) throws IOException {
        Server receivingServer = serverMap.get(message.getReceiverId());
        if (receivingServer == null || !receivingServer.isConnectedToNetwork) {
            throw new IOException("receiver is down, cannot receive msg=" + message);
        }
    }

    @Override
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
        final Function<Message, Message> messageProcessor;
        final ExecutorService requestExecutor = Executors.newCachedThreadPool();

        Server(Function<Message, Message> messageProcessor) {
            this.messageProcessor = messageProcessor;
        }

        private void add(Message msg) throws IOException {
            if (!isRunning) {
                Future<Message> msgFuture = callbackMap.remove(msg.getId());
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
                    LOGGER.debug("IN (request) : {}", message);
                    String correlationId = message.getId();
                    if (correlationId == null) {
                        LOGGER.error("correlationId must not be null");
                        continue;
                    }
                    if (!isRunning || correlationId.equals("closingServer")) {
                        messageQueue.clear();
                        break;
                    }

                    // we also test if a received message can be serialized and deserialized successfully.
                    byte[] objBytes = SerializationUtil.serialize(message);
                    Message request = SerializationUtil.deserialize(objBytes);

                    // don't block request processing
                    requestExecutor.submit(() -> {
                        // we first process the message
                        Message response = messageProcessor.apply(request);

                        // we send the response to the callback
                        CompletableFuture<Message> msgFuture = callbackMap.remove(request.getId());
                        if (msgFuture != null) {
                            LOGGER.debug("OUT (response) : {}", response);
                            msgFuture.complete(response);
                        }
                    });
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

        public synchronized void shutdown() {
            isConnectedToNetwork = false;
            isRunning = false;
            requestExecutor.shutdown();
            messageQueue.offer(new Message() {
            }.setCorrelationId("closingServer"));
        }
    }
}
