package com.mboysan.consensus;

import java.io.IOException;

class BizurClient extends AbstractClient implements BizurRPC {
    public BizurClient(Transport transport) {
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
}
