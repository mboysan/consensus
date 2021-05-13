package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Message;
import com.mboysan.dist.Transport;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class RaftClient implements RaftRPC {

    final Transport transport;
    final int senderId;
    final int receiverId;

    public RaftClient(Transport transport, int senderId, int receiverId) {
        this.transport = transport;
        this.senderId = senderId;
        this.receiverId = receiverId;
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return (RequestVoteResponse) sendRecv(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return (AppendEntriesResponse) sendRecv(request);
    }

    @Override
    public boolean stateMachineRequest(String clientCommand) {
        return (Boolean) sendRecv(clientCommand);
    }

    Serializable sendRecv(Serializable request) {
        String corrId = UUID.randomUUID().toString();
        Message message = new Message(corrId, senderId, receiverId, request);
        try {
            return transport.sendRecvAsync(message).get().getCommand();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
