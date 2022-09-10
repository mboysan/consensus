package com.mboysan.consensus;

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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class KVStoreTestBase {

    void putGetSequentialTest() throws Exception {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            String val = "testVal" + i;
            getRandomClient().set(key, val);
            expectedEntries.put(key, val);
        }
        assertEntriesForAll(expectedEntries);
    }

    void deleteSequentialTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            getRandomClient().set(key, "val" + i);
            getRandomClient().delete(key);
        }
        assertStoreSizeForAll(0);
    }

    void multiThreadTest() throws Exception {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        try (MultiThreadExecutor exec = new MultiThreadExecutor()) {
            for (int i = 0; i < 100; i++) {
                int finalI = i;
                exec.execute(() -> {
                    String key = "testKey" + finalI;
                    String val = "testVal" + finalI;
                    getRandomClient().set(key ,val);
                    if (RngUtil.nextBoolean()) {   // in some cases, remove the entry
                        getRandomClient().delete(key);
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
            awaiting(() -> getClient(anotherStore).set(expKey, expVal));    // allow sync time
            expectedEntries.put(expKey, expVal);

            connect(storeToDisconnect);

            awaiting(KVOperationException.class, () -> assertEquals(expVal, getClient(storeToDisconnect).get(expKey)));
        }
        awaiting(() -> assertEntriesForAll(expectedEntries)); // allow sync time
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
    KVStoreClient getRandomClient() {
        return getClient(RngUtil.nextInt(getClients().length));
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

    void assertEntriesForAll(Map<String, String> expectedEntries) throws KVOperationException {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (int i = 0; i < getClients().length; i++) {
                assertEquals(expectedEntries.get(expKey), getClient(i).get(expKey));
            }
        }
    }

    void assertStoreSizeForAll(int size) throws KVOperationException {
        for (int i = 0; i < getClients().length; i++) {
            assertEquals(size, getClient(i).iterateKeys().size());
        }
    }
}
