package com.mboysan.consensus;

import com.mboysan.consensus.message.*;

import java.io.IOException;

interface BizurRPC extends RPCProtocol {
    HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException;

    PleaseVoteResponse pleaseVote(PleaseVoteRequest request) throws IOException;

    ReplicaReadResponse replicaRead(ReplicaReadRequest request) throws IOException;

    ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) throws IOException;

    KVGetResponse get(KVGetRequest request) throws IOException;

    KVSetResponse set(KVSetRequest request) throws IOException;

    KVDeleteResponse delete(KVDeleteRequest request) throws IOException;

    KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException;

    @Override
    default Message processRequest(Message message) throws IOException {
        if (message instanceof HeartbeatRequest request) {
            return heartbeat(request);
        } else if (message instanceof PleaseVoteRequest request) {
            return pleaseVote(request);
        } else if (message instanceof ReplicaReadRequest request) {
            return replicaRead(request);
        } else if (message instanceof ReplicaWriteRequest request) {
            return replicaWrite(request);
        } else if (message instanceof KVGetRequest request) {
            return get(request);
        } else if (message instanceof KVSetRequest request) {
            return set(request);
        } else if (message instanceof KVDeleteRequest request) {
            return delete(request);
        } else if (message instanceof KVIterateKeysRequest request) {
            return iterateKeys(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }
}
