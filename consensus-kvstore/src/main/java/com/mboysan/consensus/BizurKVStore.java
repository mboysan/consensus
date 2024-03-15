package com.mboysan.consensus;

import com.mboysan.consensus.message.BizurKVDeleteRequest;
import com.mboysan.consensus.message.BizurKVDeleteResponse;
import com.mboysan.consensus.message.BizurKVGetRequest;
import com.mboysan.consensus.message.BizurKVGetResponse;
import com.mboysan.consensus.message.BizurKVIterateKeysRequest;
import com.mboysan.consensus.message.BizurKVIterateKeysResponse;
import com.mboysan.consensus.message.BizurKVSetRequest;
import com.mboysan.consensus.message.BizurKVSetResponse;
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

public class BizurKVStore extends AbstractKVStore<BizurNode> {

    public BizurKVStore(BizurNode node, Transport clientServingTransport) {
        super(node, clientServingTransport);
    }

    @Override
    void dumpStoreMetricsAsync() {
        getNode().dumpMetricsAsync();
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            BizurKVGetResponse response = getNode().get(new BizurKVGetRequest(request.getKey()));
            return new KVGetResponse(response.isSuccess(), response.getException(), response.getValue());
        } catch (Exception e) {
            logError(request, e);
            return new KVGetResponse(false, e, null);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            BizurKVSetResponse response = getNode().set(new BizurKVSetRequest(request.getKey(), request.getValue()));
            return new KVSetResponse(response.isSuccess(), response.getException());
        } catch (Exception e) {
            logError(request, e);
            return new KVSetResponse(false, e);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            BizurKVDeleteResponse response = getNode().delete(new BizurKVDeleteRequest(request.getKey()));
            return new KVDeleteResponse(response.isSuccess(), response.getException());
        } catch (Exception e) {
            logError(request, e);
            return new KVDeleteResponse(false, e);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            BizurKVIterateKeysResponse response = getNode().iterateKeys(new BizurKVIterateKeysRequest());
            return new KVIterateKeysResponse(response.isSuccess(), response.getException(), response.getKeys());
        } catch (Exception e) {
            logError(request, e);
            return new KVIterateKeysResponse(false, e, null);
        }
    }

    @Override
    public CustomResponse customRequest(CustomRequest request) {
        try {
            return getNode().customRequest(request);
        } catch (Exception e) {
            logError(request, e);
            return new CustomResponse(false, e, null);
        }
    }
}
