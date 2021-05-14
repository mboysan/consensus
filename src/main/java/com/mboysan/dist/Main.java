package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.RaftServer;
import com.mboysan.dist.consensus.raft.StateMachineRequest;

public class Main {

    public static void main(String[] args) {
        Transport transport = new InVMTransport();

        RaftServer rs1 = new RaftServer(1, transport);
        RaftServer rs2 = new RaftServer(2, transport);
        RaftServer rs3 = new RaftServer(3, transport);

        boolean result = rs1.stateMachineRequest(new StateMachineRequest("set=myKey,val=myVal")).isApplied();
        System.out.println("result=" + result);
    }
}
