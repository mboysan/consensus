package com.mboysan.consensus;

import com.mboysan.consensus.message.CommandException;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVOperationResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.util.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class KVStoreClient extends AbstractClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClient.class);

    public KVStoreClient(Transport transport) {
        super(transport);
    }

    public void start() throws IOException {
        getTransport().start();
    }
    public void shutdown() {
        getTransport().shutdown();
        LOGGER.info("client shutdown");
    }

    public void set(String key, String value) throws KVOperationException {
        exec(() -> {
            KVSetRequest request = new KVSetRequest(key, value).setReceiverId(nextNodeId());
            KVSetResponse response = (KVSetResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return null;
        });
    }

    public String get(String key) throws KVOperationException {
        return exec(() -> {
            KVGetRequest request = new KVGetRequest(key).setReceiverId(nextNodeId());
            KVGetResponse response = (KVGetResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return response.getValue();
        });
    }

    public void delete(String key) throws KVOperationException {
        exec(() -> {
            KVDeleteRequest request = new KVDeleteRequest(key).setReceiverId(nextNodeId());
            KVDeleteResponse response = (KVDeleteResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return null;
        });
    }

    public Set<String> iterateKeys() throws KVOperationException {
        return exec(() -> {
            KVIterateKeysRequest request = new KVIterateKeysRequest().setReceiverId(nextNodeId());
            KVIterateKeysResponse response = (KVIterateKeysResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return response.getKeys();
        });
    }

    private void validateResponse(KVOperationResponse response) throws Exception {
        if (!response.isSuccess()) {
            Exception e = response.getException();
            if (e != null) {
                throw e;
            }
        }
    }

    private <T> T exec(ThrowingSupplier<T> supplier) throws KVOperationException {
        try {
            return supplier.get();
        } catch (Throwable e) {
            throw new KVOperationException(e);
        }
    }

    public String customRequest(String command) throws CommandException {
        return customRequest(command, -1);
    }

    public String customRequest(String command, int routeTo) throws CommandException {
        try {
            CustomRequest request = new CustomRequest(command)
                    .setRouteTo(routeTo)
                    .setReceiverId(nextNodeId());
            CustomResponse response = (CustomResponse) getTransport().sendRecv(request);
            if (!response.isSuccess()) {
                Exception e = response.getException();
                if (e != null) {
                    throw new CommandException(e);
                }
            }
            return response.getPayload();
        } catch (Exception e) {
            throw new CommandException(e);
        }
    }
}
