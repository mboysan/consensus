package com.mboysan.dist;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class InVMTransport implements Transport, AutoCloseable {

    private final ExecutorService serverExecutor = Executors.newCachedThreadPool();
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool();

    private final Map<Integer, Server> serverMap = new ConcurrentHashMap<>();
    private final Map<String, Callback<Message>> callbackMap = new ConcurrentHashMap<>();

    @Override
    public void addServer(int nodeId, ProtocolRPC protoServer) {
        Server server = new Server(protoServer);
        // add this server to map and start processing
        serverMap.put(nodeId, server);
        // create clients for other servers on this server
        serverMap.forEach((serverId, otherServer) -> {
            server.clientMap.put(serverId, protoServer.createClient(this, nodeId, serverId));
        });
        // create this server's client on other servers
        serverMap.forEach((serverId, otherServer) -> {
            otherServer.clientMap.put(nodeId, protoServer.createClient(this, serverId, nodeId));
        });
        serverExecutor.execute(server);
    }

    public void sendForEach(int senderId, Consumer<ProtocolRPC> protoClient) {
        serverMap.get(senderId).clientMap.forEach((clientId, client) -> {
            protoClient.accept(client);
        });
    }

    @Override
    public void send(int senderId, int receiverId, Consumer<ProtocolRPC> protoClient) {
        ProtocolRPC client = serverMap.get(senderId).clientMap.get(receiverId);
        protoClient.accept(client);
    }

    @Override
    public Future<Message> sendRecvAsync(Message message) {
        Callback<Message> msgCallback = new Callback<>();
        callbackMap.put(message.getCorrelationId(), msgCallback);
        Future<Message> respFuture = callbackExecutor.submit(msgCallback::get);
        serverMap.get(message.getReceiverId()).add(message);
        return respFuture;
    }

    @Override
    public int getServerCount() {
        return serverMap.size();
    }

    @Override
    public void close() {
        serverExecutor.shutdown();
        callbackExecutor.shutdown();
        callbackMap.clear();
        serverMap.forEach((i, server) -> server.close());
        serverMap.clear();
    }

    static class Callback<T> implements Supplier<T>, Consumer<T> {
        private T objToSupply = null;
        @Override
        public synchronized void accept(T r) {
            objToSupply = r;
            notify();
        }
        @Override
        public synchronized T get() {
            while (objToSupply == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return objToSupply;
        }
    }

    class Server implements Runnable, AutoCloseable {
        volatile boolean isRunning = true;
        final BlockingDeque<Message> messageQueue = new LinkedBlockingDeque<>();
        final ProtocolRPC protoServer;
        final Map<Integer, ProtocolRPC> clientMap = new ConcurrentHashMap<>();

        Server(ProtocolRPC protoServer) {
            this.protoServer = protoServer;
        }

        private void add(Message msg) {
            messageQueue.offer(msg);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Message message = messageQueue.take();
                    String correlationId = message.getCorrelationId();
                    if (correlationId == null) {
                        System.err.println("correlationId cannot be null");
                        continue;
                    }
                    if (correlationId.equals("closingServer")) {
                        continue;   // this will force the loop to check for isRunning=false
                    }

                    // we first process the message
                    Serializable response = protoServer.apply(message.getCommand());

                    // we send the response to the callback
                    Callback<Message> msgCallback = callbackMap.remove(message.getCorrelationId());
                    if (msgCallback != null) {
                        Message respMsg = new Message(correlationId, message.getReceiverId(), message.getSenderId(), response);
                        msgCallback.accept(respMsg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public synchronized void close() {
            isRunning = false;
            messageQueue.offer(new Message("closingServer", -1, -1, null));
            messageQueue.clear();
        }
    }

}
