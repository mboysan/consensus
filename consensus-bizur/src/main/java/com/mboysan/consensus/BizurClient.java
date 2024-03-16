package com.mboysan.consensus;

import com.mboysan.consensus.message.CheckBizurIntegrityRequest;
import com.mboysan.consensus.message.CheckBizurIntegrityResponse;
import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.HeartbeatRequest;
import com.mboysan.consensus.message.HeartbeatResponse;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;

import java.io.IOException;

class BizurClient extends AbstractClient {
    BizurClient(Transport transport) {
        super(transport);
    }

    public HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException {
        return (HeartbeatResponse) getTransport().sendRecv(request);
    }

    public PleaseVoteResponse pleaseVote(PleaseVoteRequest request) throws IOException {
        return (PleaseVoteResponse) getTransport().sendRecv(request);
    }

    public ReplicaReadResponse replicaRead(ReplicaReadRequest request) throws IOException {
        return (ReplicaReadResponse) getTransport().sendRecv(request);
    }

    public ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) throws IOException {
        return (ReplicaWriteResponse) getTransport().sendRecv(request);
    }

    public CollectKeysResponse collectKeys(CollectKeysRequest request) throws IOException {
        return (CollectKeysResponse) getTransport().sendRecv(request);
    }

    public CheckBizurIntegrityResponse checkBizurIntegrity(CheckBizurIntegrityRequest request) throws IOException {
        return (CheckBizurIntegrityResponse) getTransport().sendRecv(request);
    }
}
