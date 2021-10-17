package com.mboysan.consensus;

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
    default Message processRequest(Message request) throws IOException {
        if (request instanceof HeartbeatRequest) {
            return heartbeat((HeartbeatRequest) request);
        } else if (request instanceof PleaseVoteRequest) {
            return pleaseVote((PleaseVoteRequest) request);
        } else if (request instanceof ReplicaReadRequest) {
            return replicaRead((ReplicaReadRequest) request);
        } else if (request instanceof ReplicaWriteRequest) {
            return replicaWrite((ReplicaWriteRequest) request);
        } else if (request instanceof KVGetRequest) {
            return get((KVGetRequest) request);
        } else if (request instanceof KVSetRequest) {
            return set((KVSetRequest) request);
        } else if (request instanceof KVDeleteRequest) {
            return delete((KVDeleteRequest) request);
        } else if (request instanceof KVIterateKeysRequest) {
            return iterateKeys((KVIterateKeysRequest) request);
        }
        throw new IllegalArgumentException("unrecognized message");
    }
}
