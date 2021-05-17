package com.mboysan.dist;

import com.mboysan.dist.consensus.raft.RaftServer;
import com.mboysan.dist.consensus.raft.RequestVoteRequest;
import com.mboysan.dist.consensus.raft.RequestVoteResponse;
import com.mboysan.dist.consensus.raft.StateMachineRequest;

public class Main {

    public static void main(String[] args) throws Exception {
        Transport transport = new InVMTransport();

        RaftServer rs1 = new RaftServer(1, transport);
        RaftServer rs2 = new RaftServer(2, transport);
        RaftServer rs3 = new RaftServer(3, transport);

        try {
            boolean result = rs1.stateMachineRequest(new StateMachineRequest("set=myKey,val=myVal")).isApplied();
            System.out.println("result=" + result);
        } catch (Exception e) {
            System.err.println(e);
        }

        RequestVoteResponse resp = rs1.getRPC(transport).requestVote(new RequestVoteRequest(1, 1, 1, 1).setSenderId(1).setReceiverId(2));

        transport.close();
    }
}
