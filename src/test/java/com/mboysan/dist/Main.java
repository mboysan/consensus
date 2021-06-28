package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.RaftServer;
import com.mboysan.dist.consensus.raft.StateMachineRequest;
import com.mboysan.dist.consensus.raft.StateMachineResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Map<Integer, String> destinations = Map.of(
                0, "localhost:8080",
                1, "localhost:8081",
                2, "localhost:8082"
        );

        Transport transport1 = new NettyTransport(8080, destinations);
        Transport transport2 = new NettyTransport(8081, destinations);
        Transport transport3 = new NettyTransport(8082, destinations);

        RaftServer server1 = new RaftServer(0, transport1);
        RaftServer server2 = new RaftServer(1, transport2);
        RaftServer server3 = new RaftServer(2, transport3);

        transport1.start();
        transport2.start();
        transport3.start();


        List<Future<Void>> serverFutures = new ArrayList<>();
        serverFutures.add(server1.start());
        serverFutures.add(server2.start());
        serverFutures.add(server3.start());

        for (Future<Void> serverFuture : serverFutures) {
            serverFuture.get();
        }

        System.out.println();

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
        transport1.shutdown();
        transport2.shutdown();
        transport3.shutdown();
    }

    public static void main2(String[] args) throws Exception {
        Transport transport = new InVMTransport();

        int serverCount = 3;

        RaftServer[] servers = new RaftServer[serverCount];
        List<Future<Void>> serverFutures = new ArrayList<>();
        for (int i = 0; i < serverCount; i++) {
            int nodeId = i + 1;
            servers[i] = new RaftServer(nodeId, transport);
            serverFutures.add(servers[i].start());
        }

        for (Future<Void> serverFuture : serverFutures) {
            serverFuture.get();
        }

        int cmdCount = 10;
        for (int i = 0; i < cmdCount; i++) {
            RaftServer server = servers[0];
            StateMachineResponse resp = server.stateMachineRequest(new StateMachineRequest("test command " + i));
            System.out.println(resp);
        }

        Thread.sleep(10000);

        System.out.println("ENDING PROGRAM");
        transport.shutdown();

        for (int i = 0; i < serverCount; i++) {
            servers[i].shutdown();
        }

        System.out.println("PROGRAM ENDED");
    }
}
