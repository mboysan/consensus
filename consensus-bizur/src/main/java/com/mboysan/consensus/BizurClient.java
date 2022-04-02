package com.mboysan.consensus;

import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.HeartbeatRequest;
import com.mboysan.consensus.message.HeartbeatResponse;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;

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
    public CollectKeysResponse collectKeys(CollectKeysRequest request) throws IOException {
        return (CollectKeysResponse) getTransport().sendRecv(request);
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
