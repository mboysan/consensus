package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.*;

public class Main {

    public static void main(String[] args) throws Exception {
        Transport transport = new InVMTransport();

        RaftServer rs1 = new RaftServer(1, transport);
        RaftServer rs2 = new RaftServer(2, transport);
//        RaftServer rs3 = new RaftServer(3, transport);

        Thread trs1 = new Thread(rs1, "RaftServer1");
//        Thread trs2 = new Thread(rs2, "RaftServer2");
//        Thread trs3 = new Thread(rs3, "RaftServer3");

        trs1.start();
//        trs2.start();
//        trs3.start();

//        Thread.sleep(100000);
        Thread.sleep(10000);

        StateMachineResponse resp1 = rs1.stateMachineRequest(new StateMachineRequest("test command"));
        StateMachineResponse resp2 = rs1.stateMachineRequest(new StateMachineRequest("test command2"));
        StateMachineResponse resp3 = rs1.stateMachineRequest(new StateMachineRequest("test command3"));
        StateMachineResponse resp4 = rs1.stateMachineRequest(new StateMachineRequest("test command4"));

        Thread.sleep(10000);

        System.out.println("ENDING PROGRAM");
        transport.close();
        rs1.close();
        System.out.println("PROGRAM ENDED");
    }
}
