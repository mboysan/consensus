package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class BizurKVStore implements KVStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(BizurKVStore.class);

    private final BizurNode bizur;

    public BizurKVStore(int nodeId, Transport transport, int numBuckets) {
        this(new BizurNode(nodeId, transport, numBuckets));
    }

    public BizurKVStore(int nodeId, Transport transport) {
        this(new BizurNode(nodeId, transport));
    }

    BizurKVStore(BizurNode bizur) {
        this.bizur = bizur;
    }

    @Override
    public synchronized void start() throws Exception {
        bizur.start().get();
    }

    @Override
    public synchronized void shutdown() {
        bizur.shutdown();
    }

    @Override
    public boolean put(String key, String value) {
        return exec(() -> {
            bizur.set(key, value).get();
            return true;
        });
    }

    @Override
    public String get(String key) {
        return exec(() -> bizur.get(key).get());
    }

    @Override
    public boolean remove(String key) {
        return exec(() -> {
            bizur.delete(key).get();
            return true;
        });
    }

    @Override
    public Set<String> keySet() {
        return exec(() -> bizur.iterateKeys().get());
    }

    private <T> T exec(CheckedSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }
}
