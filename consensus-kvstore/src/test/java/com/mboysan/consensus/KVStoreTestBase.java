package com.mboysan.consensus;

import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVOperationResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.util.RngUtil;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class KVStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreTestBase.class);

    void putGetSequentialTest() {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            String val = "testVal" + i;
            setAwaiting(getRandomClientId(), key, val);
            expectedEntries.put(key, val);
        }
        assertEntriesForAll(expectedEntries);
    }

    void deleteSequentialTest() {
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            setAwaiting(getRandomClientId(), key, "val" + i);
            deleteAwaiting(getRandomClientId(), key);
        }
        assertStoreSizeForAll(0);
    }

    void multiThreadTest() throws Exception {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        try (MultiThreadExecutor exec = new MultiThreadExecutor(4)) {
            for (int i = 0; i < 100; i++) {
                int finalI = i;
                exec.execute(() -> {
                    String key = "testKey" + finalI;
                    String val = "testVal" + finalI;
                    setAwaiting(getRandomClientId(), key, val);
                    if (RngUtil.nextBoolean()) {   // in some cases, remove the entry
                        deleteAwaiting(getRandomClientId(), key);
                    } else {    // in other cases, just leave it inserted.
                        expectedEntries.put(key, val);
                    }
                });
            }
        }
        assertEntriesForAll(expectedEntries);
    }

    void storeFailureSequentialTest() {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        int numStores = getStores().length;

        for (int storeId = 0; storeId < numStores; storeId++) {
            int storeToDisconnect = storeId;
            disconnect(storeToDisconnect);
            assertThrows(KVOperationException.class, () -> getClient(storeToDisconnect).set("k0", "v0"));

            int anotherStore = (storeToDisconnect + 1) % numStores;
            String expKey = "k" + storeToDisconnect;
            String expVal = "v" + storeToDisconnect;
            awaiting(() -> setAwaiting(anotherStore, expKey, expVal)); // allow sync time
            expectedEntries.put(expKey, expVal);

            connect(storeToDisconnect);

            awaiting(KVOperationException.class, () -> assertEquals(expVal, getClient(storeToDisconnect).get(expKey)));
        }
        awaiting(() -> assertEntriesForAll(expectedEntries)); // allow sync time
    }

    void dumpStoreMetricsTest() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(4);

        AtomicLong actualSizeOfKeys = new AtomicLong(0);
        AtomicLong actualSizeOfValues = new AtomicLong(0);
        AtomicLong actualSizeTotal = new AtomicLong(0);
        Consumer<MeasurementEvent> eventConsumer = event -> {
            LOGGER.info("captured measurement=[{}]", event);
            try {
                switch (event.getName()) {
                    case Constants.Metrics.INSIGHTS_STORE_SIZE_OF_KEYS -> {
                        actualSizeOfKeys.addAndGet((long) event.getPayload());
                        barrier.await();
                    }
                    case Constants.Metrics.INSIGHTS_STORE_SIZE_OF_VALUES -> {
                        actualSizeOfValues.addAndGet((long) event.getPayload());
                        barrier.await();
                    }
                    case Constants.Metrics.INSIGHTS_STORE_SIZE_OF_TOTAL -> {
                        actualSizeTotal.addAndGet((long) event.getPayload());
                        barrier.await();
                    }
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                LOGGER.error(e.getMessage(), e);
            }
        };
        EventManagerService.getInstance().register(MeasurementEvent.class, eventConsumer);
        try {
            // check all stores have zero total size
            for (AbstractKVStore<?> store : getStores()) {
                store.dumpStoreMetricsAsync();
                barrier.await();
                barrier.reset();
                assertEquals(0, actualSizeOfKeys.get());
                assertEquals(0, actualSizeOfValues.get());
                assertEquals(0, actualSizeTotal.get());
            }

            // put some values
            final int totalNumKeys = 100;
            long expectedSizeOfKeys = 0;
            long expectedSizeOfValues = 0;
            long expectedTotalSize = 0;
            for (int i = 0; i < totalNumKeys; i++) {
                String key = UUID.randomUUID().toString() + RngUtil.nextInt(Integer.MAX_VALUE);
                String val = UUID.randomUUID().toString() + RngUtil.nextInt(Integer.MAX_VALUE);
                setAwaiting(getRandomClientId(), key, val);
                expectedSizeOfKeys += key.length();
                expectedSizeOfValues += val.length();
                expectedTotalSize += (key.length() + val.length());
            }

            for (int clientId = 0; clientId < getClients().length; clientId++) {
                assertEquals(totalNumKeys, iterateKeysAwaiting(clientId).size());
            }

            // check all stores have reported the same metrics
            for (AbstractKVStore<?> store : getStores()) {
                store.dumpStoreMetricsAsync();
                barrier.await();
                barrier.reset();
                assertEquals(expectedSizeOfKeys, actualSizeOfKeys.get(), 0);
                assertEquals(expectedSizeOfValues, actualSizeOfValues.get(), 0);
                assertEquals(expectedTotalSize, actualSizeTotal.get(), 0);
    
                actualSizeOfKeys.set(0);
                actualSizeOfValues.set(0);
                actualSizeTotal.set(0);
            }
        } finally {
            EventManagerService.getInstance().deregister(eventConsumer);
        }
    }

    void testFailedResponses(AbstractKVStore<?> storeWithMockedNode) throws IOException {
        KVOperationResponse response;
        response = storeWithMockedNode.get(new KVGetRequest("a"));
        Assertions.assertTrue(response.getException() instanceof IOException);

        response = storeWithMockedNode.set(new KVSetRequest("a", "b"));
        Assertions.assertTrue(response.getException() instanceof IOException);

        response = storeWithMockedNode.delete(new KVDeleteRequest("a"));
        Assertions.assertTrue(response.getException() instanceof IOException);

        response = storeWithMockedNode.iterateKeys(new KVIterateKeysRequest());
        Assertions.assertTrue(response.getException() instanceof IOException);

        CustomResponse resp = storeWithMockedNode.customRequest(new CustomRequest(""));
        Assertions.assertTrue(resp.getException() instanceof IOException);
    }

    abstract KVStoreClient[] getClients();
    abstract AbstractKVStore<?>[] getStores();
    abstract InVMTransport getNodeServingTransport();
    abstract InVMTransport getClientServingTransport(int storeId);


    int getRandomClientId() {
        return RngUtil.nextInt(getClients().length);
    }

    KVStoreClient getRandomClient() {
        return getClient(getRandomClientId());
    }
    KVStoreClient getClient(int clientId) {
        return getClients()[clientId];
    }

    private void disconnect(int storeId) {
        getClientServingTransport(storeId).connectedToNetwork(storeId, false);
        getNodeServingTransport().connectedToNetwork(storeId, false);
    }
    private void connect(int storeId) {
        getClientServingTransport(storeId).connectedToNetwork(storeId, true);
        getNodeServingTransport().connectedToNetwork(storeId, true);
    }

    void assertEntriesForAll(Map<String, String> expectedEntries) {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (int clientId = 0; clientId < getClients().length; clientId++) {
                assertEquals(expectedEntries.get(expKey), getAwaiting(clientId, expKey));
            }
        }
    }

    void assertStoreSizeForAll(int size) {
        for (int clientId = 0; clientId < getClients().length; clientId++) {
            assertEquals(size, iterateKeysAwaiting(clientId).size());
        }
    }

    private String getAwaiting(int clientId, String key) {
        return awaiting(TimeoutException.class, () -> getClient(clientId).get(key));
    }

    private void setAwaiting(int clientId, String key, String value) {
        awaiting(TimeoutException.class, () -> getClient(clientId).set(key, value));
    }

    private void deleteAwaiting(int clientId, String key) {
        awaiting(TimeoutException.class, () -> getClient(clientId).delete(key));
    }

    private Set<String> iterateKeysAwaiting(int clientId) {
        return awaiting(TimeoutException.class, () -> getClient(clientId).iterateKeys());
    }
}
