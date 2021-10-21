package com.mboysan.consensus;

import com.mboysan.consensus.message.AppendEntriesRequest;
import com.mboysan.consensus.message.AppendEntriesResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.RequestVoteRequest;
import com.mboysan.consensus.message.RequestVoteResponse;
import com.mboysan.consensus.message.StateMachineRequest;
import com.mboysan.consensus.message.StateMachineResponse;

import java.io.IOException;

interface RaftRPC extends RPCProtocol {
    AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException;

    RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException;

    StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException;

    @Override
    default Message processRequest(Message request) throws IOException {
        if (request instanceof AppendEntriesRequest) {
            return appendEntries((AppendEntriesRequest) request);
        }
        if (request instanceof RequestVoteRequest) {
            return requestVote((RequestVoteRequest) request);
        }
        if (request instanceof StateMachineRequest) {
            return stateMachineRequest((StateMachineRequest) request);
        }
        throw new IllegalArgumentException("unrecognized message");
    }
}
