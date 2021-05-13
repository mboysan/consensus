package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.ProtocolRPC;
import com.mboysan.dist.Transport;

import java.io.Serializable;

public interface RaftRPC extends ProtocolRPC {
    AppendEntriesResponse appendEntries(AppendEntriesRequest request);
    RequestVoteResponse requestVote(RequestVoteRequest request);
    boolean stateMachineRequest(String clientCommand);

    @Override
    default ProtocolRPC createClient(Transport transport, int senderId, int receiverId) {
        return new RaftClient(transport, senderId, receiverId);
    }

    @Override
    default Serializable apply(Serializable request) {
        if (request instanceof AppendEntriesRequest) {
            return appendEntries((AppendEntriesRequest) request);
        }
        if (request instanceof RequestVoteRequest) {
            return requestVote((RequestVoteRequest) request);
        }
        if (request instanceof String) {
            return stateMachineRequest((String) request);
        }
        throw new IllegalArgumentException("unrecognized message");
    }
}
