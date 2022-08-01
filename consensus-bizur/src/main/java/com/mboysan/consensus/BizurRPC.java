package com.mboysan.consensus;

import com.mboysan.consensus.message.CollectKeysRequest;
import com.mboysan.consensus.message.CollectKeysResponse;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
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

    CollectKeysResponse collectKeys(CollectKeysRequest request) throws IOException;

    KVGetResponse get(KVGetRequest request) throws IOException;

    KVSetResponse set(KVSetRequest request) throws IOException;

    KVDeleteResponse delete(KVDeleteRequest request) throws IOException;

    KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException;

    CustomResponse customRequest(CustomRequest request) throws IOException;

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
        } else if(message instanceof  CollectKeysRequest request) {
            return collectKeys(request);
        } else if (message instanceof KVGetRequest request) {
            return get(request);
        } else if (message instanceof KVSetRequest request) {
            return set(request);
        } else if (message instanceof KVDeleteRequest request) {
            return delete(request);
        } else if (message instanceof KVIterateKeysRequest request) {
            return iterateKeys(request);
        } else if (message instanceof CustomRequest request) {
            return customRequest(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }
}
