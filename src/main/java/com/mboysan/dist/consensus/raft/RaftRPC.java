package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Message;
import com.mboysan.dist.RPCProtocol;
import com.mboysan.dist.Transport;

import java.io.IOException;
import java.io.UncheckedIOException;

public interface RaftRPC extends RPCProtocol {
    AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException;
    RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException;
    StateMachineResponse stateMachineRequest(StateMachineRequest request) throws IOException;

    @Override
    default Message apply(Message message) {
        try {
            if (message instanceof AppendEntriesRequest) {
                return appendEntries((AppendEntriesRequest) message);
            }
            if (message instanceof RequestVoteRequest) {
                return requestVote((RequestVoteRequest) message);
            }
            if (message instanceof StateMachineRequest) {
                return stateMachineRequest((StateMachineRequest) message);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        throw new IllegalArgumentException("unrecognized message");
    }

    @Override
    default RaftRPC getRPC(Transport transport) {
        return new RaftClient(transport);
    }
}
