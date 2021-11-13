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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Future;

public class BizurKVStore implements KVStoreRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurKVStore.class);

    private final BizurNode bizur;

    public BizurKVStore(BizurNode bizur) {
        this.bizur = bizur;
    }

    @Override
    public Future<Void> start() throws IOException {
        return bizur.start();
    }

    @Override
    public void shutdown() {
        bizur.shutdown();
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            return bizur.get(request);
        } catch (Exception e) {
            logError(bizur.getNodeId(), request, e);
            return new KVGetResponse(false, e, null);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            return bizur.set(request);
        } catch (Exception e) {
            logError(bizur.getNodeId(), request, e);
            return new KVSetResponse(false, e);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            return bizur.delete(request);
        } catch (Exception e) {
            logError(bizur.getNodeId(), request, e);
            return new KVDeleteResponse(false, e);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            return bizur.iterateKeys(request);
        } catch (Exception e) {
            logError(bizur.getNodeId(), request, e);
            return new KVIterateKeysResponse(false, e, null);
        }
    }

    private static void logError(int nodeId, Message request, Exception err) {
        LOGGER.error("on BizurKVStore-{}, error={}, for request={}", nodeId, err, request.toString());
    }
}
