package com.mboysan.consensus;

import com.mboysan.consensus.message.AppendEntriesRequest;
import com.mboysan.consensus.message.AppendEntriesResponse;
import com.mboysan.consensus.message.RequestVoteRequest;
import com.mboysan.consensus.message.RequestVoteResponse;
import com.mboysan.consensus.message.StateMachineRequest;
import com.mboysan.consensus.message.StateMachineResponse;

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
