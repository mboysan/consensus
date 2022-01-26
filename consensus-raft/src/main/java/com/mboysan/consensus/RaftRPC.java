package com.mboysan.consensus;

import com.mboysan.consensus.message.*;

import java.io.IOException;

interface RaftRPC extends RPCProtocol {
    AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException;

    RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException;

    StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException;

    @Override
    default Message processRequest(Message message) throws IOException {
        if (message instanceof AppendEntriesRequest request) {
            return appendEntries(request);
        }
        if (message instanceof RequestVoteRequest request) {
            return requestVote(request);
        }
        if (message instanceof StateMachineRequest request) {
            return stateMachineRequest(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }
}
