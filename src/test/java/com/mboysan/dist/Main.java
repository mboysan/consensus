package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.*;

public class Main {

    public static void main(String[] args) throws Exception {
        Transport transport = new InVMTransport();

        int serverCount = 3;

        RaftServer[] servers = new RaftServer[serverCount];
        Thread[] serverThreads = new Thread[serverCount];
        for (int i = 0; i < serverCount; i++) {
            int nodeId = i + 1;
            servers[i] = new RaftServer(nodeId, transport);
            serverThreads[i] = new Thread(servers[i], "RaftServer" + nodeId);
            serverThreads[i].start();
        }

        Thread.sleep(10000);

        int cmdCount = 10;
        for (int i = 0; i < cmdCount; i++) {
            RaftServer server = servers[0];
            StateMachineResponse resp = server.stateMachineRequest(new StateMachineRequest("test command " + i));
            System.out.println(resp);
        }

        Thread.sleep(10000);

        System.out.println("ENDING PROGRAM");
        transport.close();

        for (int i = 0; i < serverCount; i++) {
            servers[i].close();
            serverThreads[i].join();
        }

        System.out.println("PROGRAM ENDED");
    }
}
