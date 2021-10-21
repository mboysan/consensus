package com.mboysan.consensus;

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
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.PleaseVoteRequest;
import com.mboysan.consensus.message.PleaseVoteResponse;
import com.mboysan.consensus.message.ReplicaReadRequest;
import com.mboysan.consensus.message.ReplicaReadResponse;
import com.mboysan.consensus.message.ReplicaWriteRequest;
import com.mboysan.consensus.message.ReplicaWriteResponse;

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
