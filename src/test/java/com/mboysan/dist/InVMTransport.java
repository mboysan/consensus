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
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool(
            new BasicThreadFactory.Builder().namingPattern("CallbackExec-%d").daemon(true).build()
    );

    private final Map<Integer, Server> serverMap = new ConcurrentHashMap<>();
    private final Map<String, Callback<Message>> callbackMap = new ConcurrentHashMap<>();

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

    public synchronized void kill(int nodeId) {
        serverMap.get(nodeId).close();
        LOGGER.info("server-{} killed", nodeId);
    }

    public synchronized void revive(int nodeId) {
        serverExecutor.submit(serverMap.get(nodeId));
        LOGGER.info("server-{} revived", nodeId);
    }

    @Override
    public Message sendRecv(Message message) throws IOException {
        LOGGER.debug("OUT (sendRecv) : {}", message);
        if (message.getSenderId() == message.getReceiverId()) {
            return sendRecvSelf(message);
        }
        Callback<Message> msgCallback = new Callback<>();
        callbackMap.put(message.getCorrelationId(), msgCallback);
        serverMap.get(message.getReceiverId()).add(message);
        return msgCallback.get();
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) throws IOException {
        LOGGER.debug("OUT (sendRecvAsync) : {}", message);
        if (message.getSenderId() == message.getReceiverId()) {
            return new GetOnlyFuture<>(sendRecvSelf(message));
        }

        Callback<Message> msgCallback = new Callback<>();
        callbackMap.put(message.getCorrelationId(), msgCallback);
        Future<Message> respFuture = callbackExecutor.submit(msgCallback::get);
        serverMap.get(message.getReceiverId()).add(message);
        return respFuture;
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

    @Override
    public synchronized void close() {
        serverExecutor.shutdown();
        callbackExecutor.shutdown();
        callbackMap.clear();
        serverMap.forEach((i, server) -> server.close());
        serverMap.clear();
    }

    static class Callback<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private T objToSupply = null;
        public void set(T r) {
            objToSupply = r;
            latch.countDown();
        }
        public T get() throws IOException {
            try {
                if (!latch.await(DEFAULT_CALLBACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    throw new IOException("await elapsed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            LOGGER.debug("IN (callback) : {}", objToSupply);
            return objToSupply;
        }
    }

    class Server implements Runnable, AutoCloseable {
        volatile boolean isRunning = true;
        final BlockingDeque<Message> messageQueue = new LinkedBlockingDeque<>();
        final RPCProtocol protoServer;

        Server(RPCProtocol protoServer) {
            this.protoServer = protoServer;
        }

        private void add(Message msg) throws IOException {
            if (!isRunning) {
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
                    LOGGER.debug("IN : {}", message);
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
                    Callback<Message> msgCallback = callbackMap.remove(message.getCorrelationId());
                    if (msgCallback != null) {
                        msgCallback.set(response);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error(e.getMessage(), e);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

        @Override
        public synchronized void close() {
            isRunning = false;
            messageQueue.offer(new Message(){}.setCorrelationId("closingServer"));
        }
    }

    private static final class GetOnlyFuture<T> implements Future<T> {
        private final T objToGet;

        private GetOnlyFuture(T objToGet) {
            this.objToGet = objToGet;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T get() {
            return objToGet;
        }

        @Override
        public T get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }

}
