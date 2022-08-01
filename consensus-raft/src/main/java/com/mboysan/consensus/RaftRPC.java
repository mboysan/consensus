package com.mboysan.consensus;

import com.mboysan.consensus.message.AppendEntriesRequest;
import com.mboysan.consensus.message.AppendEntriesResponse;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
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

    CustomResponse customRequest(CustomRequest request) throws IOException;

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
        if (message instanceof CustomRequest request) {
            return customRequest(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }
}
