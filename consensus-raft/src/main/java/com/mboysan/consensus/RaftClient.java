package com.mboysan.consensus;

import com.mboysan.consensus.message.*;

import java.io.IOException;

class RaftClient extends AbstractClient implements RaftRPC {

    RaftClient(Transport transport) {
        super(transport);
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException {
        return (RequestVoteResponse) getTransport().sendRecv(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException {
        return (AppendEntriesResponse) getTransport().sendRecv(request);
    }

    @Override
    public StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException {
        return (StateMachineResponse) getTransport().sendRecv(request);
    }
}
