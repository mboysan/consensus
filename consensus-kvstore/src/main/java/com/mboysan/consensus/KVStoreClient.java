package com.mboysan.consensus;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class KVStoreClient extends AbstractClient {

    private final List<Integer> nodeIds;
    private final AtomicInteger currIndex = new AtomicInteger(-1);

    public KVStoreClient(Transport transport) {
        super(transport);
        this.nodeIds = new ArrayList<>(Objects.requireNonNull(transport.getDestinationNodeIds()));
    }

    public void start() throws IOException {
        getTransport().start();
    }
    public void shutdown() {
        getTransport().shutdown();
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

    private int nextNodeId() {
        return nodeIds.get(currIndex.incrementAndGet() % nodeIds.size());
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
}
