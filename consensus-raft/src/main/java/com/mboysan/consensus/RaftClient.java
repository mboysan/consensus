package com.mboysan.consensus;

import com.mboysan.consensus.message.AppendEntriesRequest;
import com.mboysan.consensus.message.AppendEntriesResponse;
import com.mboysan.consensus.message.CheckRaftIntegrityRequest;
import com.mboysan.consensus.message.CheckRaftIntegrityResponse;
import com.mboysan.consensus.message.RequestVoteRequest;
import com.mboysan.consensus.message.RequestVoteResponse;

import java.io.IOException;

class RaftClient extends AbstractClient {

    RaftClient(Transport transport) {
        super(transport);
    }

    public RequestVoteResponse requestVote(RequestVoteRequest request) throws IOException {
        return (RequestVoteResponse) getTransport().sendRecv(request);
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) throws IOException {
        return (AppendEntriesResponse) getTransport().sendRecv(request);
    }

    public CheckRaftIntegrityResponse checkRaftIntegrity(CheckRaftIntegrityRequest request) throws IOException {
        return (CheckRaftIntegrityResponse) getTransport().sendRecv(request);
    }
}
