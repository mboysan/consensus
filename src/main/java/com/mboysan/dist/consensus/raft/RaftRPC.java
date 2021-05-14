package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Message;
import com.mboysan.dist.RPCProtocol;
import com.mboysan.dist.Transport;

public interface RaftRPC extends RPCProtocol {
    AppendEntriesResponse appendEntries(AppendEntriesRequest request);
    RequestVoteResponse requestVote(RequestVoteRequest request);
    StateMachineResponse stateMachineRequest(StateMachineRequest request);

    @Override
    default Message apply(Message message) {
        if (message instanceof AppendEntriesRequest) {
            return appendEntries((AppendEntriesRequest) message);
        }
        if (message instanceof RequestVoteRequest) {
            return requestVote((RequestVoteRequest) message);
        }
        if (message instanceof StateMachineRequest) {
            return stateMachineRequest((StateMachineRequest) message);
        }
        throw new IllegalArgumentException("unrecognized message");
    }

    @Override
    default RaftRPC getRPC(Transport transport) {
        return new RaftClient(transport);
    }
}
