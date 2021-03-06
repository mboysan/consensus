package com.mboysan.consensus;

import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;

public class BizurKVStore extends AbstractKVStore<BizurNode> {

    public BizurKVStore(BizurNode node, Transport clientServingTransport) {
        super(node, clientServingTransport);
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            return getNode().get(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVGetResponse(false, e, null);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            return getNode().set(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVSetResponse(false, e);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            return getNode().delete(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVDeleteResponse(false, e);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            return getNode().iterateKeys(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVIterateKeysResponse(false, e, null);
        }
    }
}
