package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Transport;

import java.io.IOException;
import java.util.Set;

public class RaftClient implements RaftRPC {

    private final Transport transport;

    public RaftClient(Transport transport) {
        this.transport = transport;
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException {
        return (RequestVoteResponse) transport.sendRecv(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException {
        return (AppendEntriesResponse) transport.sendRecv(request);
    }

    @Override
    public StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException {
        return (StateMachineResponse) transport.sendRecv(request);
    }

    @Override
    public void onServerListChanged(Set<Integer> serverIds) {
        throw new UnsupportedOperationException("this is relevant to only the RaftServer");
    }
}
