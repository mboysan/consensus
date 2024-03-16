package com.mboysan.consensus;

import com.mboysan.consensus.message.BizurKVDeleteRequest;
import com.mboysan.consensus.message.BizurKVDeleteResponse;
import com.mboysan.consensus.message.BizurKVGetRequest;
import com.mboysan.consensus.message.BizurKVGetResponse;
import com.mboysan.consensus.message.BizurKVIterateKeysRequest;
import com.mboysan.consensus.message.BizurKVIterateKeysResponse;
import com.mboysan.consensus.message.BizurKVSetRequest;
import com.mboysan.consensus.message.BizurKVSetResponse;
import com.mboysan.consensus.message.CheckBizurIntegrityRequest;
import com.mboysan.consensus.message.CheckBizurIntegrityResponse;
import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.HeartbeatRequest;
import com.mboysan.consensus.message.HeartbeatResponse;
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
    public BizurKVGetResponse get(BizurKVGetRequest request) throws IOException {
        return (BizurKVGetResponse) getTransport().sendRecv(request);
    }

    @Override
    public BizurKVSetResponse set(BizurKVSetRequest request) throws IOException {
        return (BizurKVSetResponse) getTransport().sendRecv(request);
    }

    @Override
    public BizurKVDeleteResponse delete(BizurKVDeleteRequest request) throws IOException {
        return (BizurKVDeleteResponse) getTransport().sendRecv(request);
    }

    @Override
    public BizurKVIterateKeysResponse iterateKeys(BizurKVIterateKeysRequest request) throws IOException {
        return (BizurKVIterateKeysResponse) getTransport().sendRecv(request);
    }

    @Override
    public CheckBizurIntegrityResponse checkBizurIntegrity(CheckBizurIntegrityRequest request) throws IOException {
        return (CheckBizurIntegrityResponse) getTransport().sendRecv(request);
    }

    @Override
    public CustomResponse customRequest(CustomRequest request) throws IOException {
        return (CustomResponse) getTransport().sendRecv(request);
    }
}
