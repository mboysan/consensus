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

    BizurKVGetResponse get(BizurKVGetRequest request) throws IOException;

    BizurKVSetResponse set(BizurKVSetRequest request) throws IOException;

    BizurKVDeleteResponse delete(BizurKVDeleteRequest request) throws IOException;

    BizurKVIterateKeysResponse iterateKeys(BizurKVIterateKeysRequest request) throws IOException;

    CheckBizurIntegrityResponse checkBizurIntegrity(CheckBizurIntegrityRequest request) throws IOException;

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
        } else if (message instanceof BizurKVGetRequest request) {
            return get(request);
        } else if (message instanceof BizurKVSetRequest request) {
            return set(request);
        } else if (message instanceof BizurKVDeleteRequest request) {
            return delete(request);
        } else if (message instanceof BizurKVIterateKeysRequest request) {
            return iterateKeys(request);
        } else if (message instanceof CheckBizurIntegrityRequest request) {
            return checkBizurIntegrity(request);
        } else if (message instanceof CustomRequest request) {
            return customRequest(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }
}
