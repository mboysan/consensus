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
import com.mboysan.consensus.message.SimMessage;

public class SimKVStore extends AbstractKVStore<SimNode> {


    SimKVStore(SimNode node, Transport clientServingTransport) {
        super(node, clientServingTransport);
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            SimMessage message = mapToSimMessage(request);
            getNode().simulate(message);
            return new KVGetResponse(true, null, null).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVGetResponse(false, e, null).responseTo(request);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            SimMessage message = mapToSimMessage(request);
            getNode().simulate(message);
            return new KVSetResponse(true, null).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVSetResponse(false, e).responseTo(request);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            SimMessage message = mapToSimMessage(request);
            getNode().simulate(message);
            return new KVDeleteResponse(true, null).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVDeleteResponse(false, e).responseTo(request);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            SimMessage message = mapToSimMessage(request);
            getNode().simulate(message);
            return new KVIterateKeysResponse(true, null, null).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVIterateKeysResponse(false, e, null).responseTo(request);
        }
    }

    @Override
    public CustomResponse customRequest(CustomRequest request) {
        try {
            return getNode().customRequest(request);
        } catch (Exception e) {
            logError(request, e);
            return new CustomResponse(false, e, null).responseTo(request);
        }
    }

    private SimMessage mapToSimMessage(Message request) {
        return new SimMessage()
                .setSenderId(request.getSenderId())
                .setReceiverId(request.getReceiverId())
                .setCorrelationId(request.getCorrelationId());
    }
}