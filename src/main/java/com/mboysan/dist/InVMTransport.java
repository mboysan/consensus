package com.mboysan.dist;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class InVMTransport implements Transport, AutoCloseable {

    private final ExecutorService serverExecutor = Executors.newCachedThreadPool();
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool();

    private final Map<Integer, Server> serverMap = new ConcurrentHashMap<>();
    private final Map<String, Callback<Message>> callbackMap = new ConcurrentHashMap<>();

    @Override
    public void addServer(int nodeId, RPCProtocol protoServer) {
        serverMap.computeIfAbsent(nodeId, id -> {
            Server server = new Server(protoServer);
            // add this server to map and start processing
            serverMap.put(nodeId, server);
            serverMap.forEach((i, s) -> s.protoServer.onServerListChanged(Set.copyOf(serverMap.keySet())));
            serverExecutor.execute(server);
            return server;
        });
    }

    @Override
    public Message sendRecv(Message message) {
        try {
            return sendRecvAsync(message).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);  //fixme
        } catch (ExecutionException e) {
            throw new RuntimeException(e);  //fixme
        }
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
    public synchronized void close() {
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
        final RPCProtocol protoServer;

        Server(RPCProtocol protoServer) {
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
                    Message response = protoServer.apply(message);

                    // we send the response to the callback
                    Callback<Message> msgCallback = callbackMap.remove(message.getCorrelationId());
                    if (msgCallback != null) {
                        msgCallback.accept(response);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public synchronized void close() {
            isRunning = false;
            messageQueue.offer(new Message(){}.setCorrelationId("closingServer"));
            messageQueue.clear();
        }
    }

}
