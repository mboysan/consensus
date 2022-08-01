package com.mboysan.consensus;

import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.util.concurrent.Future;

public interface KVStoreRPC extends RPCProtocol {

    Future<Void> start() throws IOException;

    void shutdown();

    KVGetResponse get(KVGetRequest request) throws IOException;

    KVSetResponse set(KVSetRequest request) throws IOException;

    KVDeleteResponse delete(KVDeleteRequest request) throws IOException;

    KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException;

    CustomResponse customRequest(CustomRequest request) throws IOException;

    @Override
    default Message processRequest(Message message) throws IOException {
        if (message instanceof KVGetRequest request) {
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
