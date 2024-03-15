package com.mboysan.consensus;

import com.mboysan.consensus.message.CheckStoreIntegrityRequest;
import com.mboysan.consensus.message.CheckStoreIntegrityResponse;
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
import java.util.Objects;
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
            String k = Objects.requireNonNull(key, "key is required");
            String v = Objects.requireNonNull(value, "value is required");
            KVSetRequest request = new KVSetRequest(k, v).setReceiverId(nextNodeId());
            KVSetResponse response = (KVSetResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return null;
        });
    }

    public String get(String key) throws KVOperationException {
        return exec(() -> {
            String k = Objects.requireNonNull(key, "key is required");
            KVGetRequest request = new KVGetRequest(k).setReceiverId(nextNodeId());
            KVGetResponse response = (KVGetResponse) getTransport().sendRecv(request);
            validateResponse(response);
            return response.getValue();
        });
    }

    public void delete(String key) throws KVOperationException {
        exec(() -> {
            String k = Objects.requireNonNull(key, "key is required");
            KVDeleteRequest request = new KVDeleteRequest(k).setReceiverId(nextNodeId());
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

    private void validateResponse(KVOperationResponse response) throws KVOperationException {
        if (!response.isSuccess()) {
            throw new KVOperationException(response.getException());
        }
    }

    private <T> T exec(ThrowingSupplier<T> supplier) throws KVOperationException {
        try {
            return supplier.get();
        } catch (Throwable e) {
            throw new KVOperationException(e);
        }
    }

    public String checkIntegrity(int level, int routeTo) throws CommandException {
        try {
            CheckStoreIntegrityRequest request = new CheckStoreIntegrityRequest(routeTo, level)
                    .setReceiverId(nextNodeId());
            CheckStoreIntegrityResponse response = (CheckStoreIntegrityResponse) getTransport().sendRecv(request);
            if (!response.isSuccess()) {
                throw new CommandException(response.getException());
            }
            return response.toString();
        } catch (Exception e) {
            throw new CommandException(e);
        }
    }

    public String customRequest(String command, String arguments, int routeTo) throws CommandException {
        try {
            String cmd = Objects.requireNonNull(command, "command is required");
            CustomRequest request = new CustomRequest(routeTo, cmd, arguments)
                    .setReceiverId(nextNodeId());
            CustomResponse response = (CustomResponse) getTransport().sendRecv(request);
            if (!response.isSuccess()) {
                throw new CommandException(response.getException());
            }
            return response.getPayload();
        } catch (Exception e) {
            throw new CommandException(e);
        }
    }
}
