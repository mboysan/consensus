package com.mboysan.consensus;

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
import java.util.Set;
import java.util.concurrent.Future;

public interface KVStoreRPC extends RPCProtocol {

    Future<Void> start() throws IOException;

    void shutdown();

    KVGetResponse get(KVGetRequest request) throws IOException;

    KVSetResponse set(KVSetRequest request) throws IOException;

    KVDeleteResponse delete(KVDeleteRequest request) throws IOException;

    KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) throws IOException;

    @Override
    default void onNodeListChanged(Set<Integer> serverIds) {
        // nothing needs to be done
    }

    @Override
    default Message processRequest(Message request) throws IOException {
        if (request instanceof KVGetRequest) {
            return get((KVGetRequest) request);
        } else if (request instanceof KVSetRequest) {
            return set((KVSetRequest) request);
        } else if (request instanceof KVDeleteRequest) {
            return delete((KVDeleteRequest) request);
        } else if (request instanceof KVIterateKeysRequest) {
            return iterateKeys((KVIterateKeysRequest) request);
        }
        throw new IllegalArgumentException("unrecognized message=" + request);
    }

}
