package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.RaftServer;
import com.mboysan.dist.consensus.raft.StateMachineRequest;
import com.mboysan.dist.consensus.raft.StateMachineResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) throws Exception {
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
