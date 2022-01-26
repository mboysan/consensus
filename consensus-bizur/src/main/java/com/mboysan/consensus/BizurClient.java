package com.mboysan.consensus;

import com.mboysan.consensus.message.*;

import java.io.IOException;

class BizurClient extends AbstractClient implements BizurRPC {
    BizurClient(Transport transport) {
        super(transport);
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException {
        return (HeartbeatResponse) getTransport().sendRecv(request);
    }

    @Override
    public PleaseVoteResponse pleaseVote(PleaseVoteRequest request) throws IOException {
        return (PleaseVoteResponse) getTransport().sendRecv(request);
    }

    @Override
    public ReplicaReadResponse replicaRead(ReplicaReadRequest request) throws IOException {
        return (ReplicaReadResponse) getTransport().sendRecv(request);
    }

    @Override
    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) throws IOException {
        return (ReplicaWriteResponse) getTransport().sendRecv(request);
    }

    @Override
    public KVGetResponse get(KVGetRequest request) throws IOException {
        return (KVGetResponse) getTransport().sendRecv(request);
    }

    @Override
    public KVSetResponse set(KVSetRequest request) throws IOException {
        return (KVSetResponse) getTransport().sendRecv(request);
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) throws IOException {
        return (KVDeleteResponse) getTransport().sendRecv(request);
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException {
        return (KVIterateKeysResponse) getTransport().sendRecv(request);
    }
}
