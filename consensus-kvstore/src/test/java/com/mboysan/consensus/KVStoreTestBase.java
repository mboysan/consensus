package com.mboysan.consensus;

import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class KVStoreTestBase<N extends AbstractNode<?>> extends NodeTestBase<N> {

    private KVStoreRPC[] stores;

    @Override
    void init(int numServers) throws Exception {
        super.init(numServers);
        stores = new KVStoreRPC[numServers];
        for (int i = 0; i < numServers; i++) {
            stores[i] = createKVStore(getNode(i));
            stores[i].start();
        }
    }

    abstract KVStoreRPC createKVStore(N node);

    @Test
    void testPutGet() throws Exception {
        init(5);

        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            String val = "testVal" + i;
            assertTrue(set(randomNodeId(), key, val));
            expectedEntries.put(key, val);
        }
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }

    @Test
    void testRemove() throws Exception {
        init(5);

        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            assertTrue(set(randomNodeId(), key, "testVal" + i));
            assertTrue(delete(randomNodeId(), key));
        }
        advanceTimeForElections();  // sync
        assertStoreSizeForAll(0);
    }

    @Test
    void multiThreadTest() throws Exception {
        init(5);

        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        MultiThreadExecutor exec = new MultiThreadExecutor();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            exec.execute(() -> {
                String key = "testKey" + finalI;
                String val = "testVal" + finalI;
                assertTrue(set(randomNodeId(), key, val));
                if (getRNG().nextBoolean()) {   // in some cases, remove the entry
                    assertTrue(delete(randomNodeId(), key));
                } else {    // in other cases, just leave it inserted.
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }

    @Test
    void testFollowerFailure() throws Exception {
        int numServers = 5;
        init(numServers);
        int leaderId = assertOneLeader();

        int totalAllowedKills = numServers / 2;
        AtomicInteger totalKilled = new AtomicInteger(0);
        ConcurrentHashMap<Integer, Object> killedNodes = new ConcurrentHashMap<>();
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        MultiThreadExecutor exec = new MultiThreadExecutor();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            exec.execute(() -> {
                String key = "testKey" + finalI;
                String val = "testVal" + finalI;

                int followerId = randomFollowerId(leaderId);
                if (getRNG().nextBoolean()) {
                    if (totalKilled.incrementAndGet() < totalAllowedKills) {
                        // try killing node
                        if (killedNodes.putIfAbsent(followerId, new Object()) == null) {
                            kill(followerId);
                            assertFalse(set(followerId, key, val));
                            assertTrue(set(leaderId, key, val)); // this shall never fail in this test
                            expectedEntries.put(key, val);
                            revive(followerId);
                            killedNodes.remove(followerId);
                        }
                        totalKilled.decrementAndGet();
                    }
                } else {
                    assertTrue(set(leaderId, key, val));
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }

    private boolean set(int nodeId, String key, String value) throws IOException {
        KVSetRequest request = new KVSetRequest(key, value).setReceiverId(nodeId);
        return stores[nodeId].set(request).isSuccess();
    }

    private String get(int nodeId, String key) throws IOException {
        KVGetRequest request = new KVGetRequest(key).setReceiverId(nodeId);
        return stores[nodeId].get(request).getValue();
    }

    private boolean delete(int nodeId, String key) throws IOException {
        KVDeleteRequest request = new KVDeleteRequest(key).setReceiverId(nodeId);
        return stores[nodeId].delete(request).isSuccess();
    }

    private Set<String> iterateKeys(int nodeId) throws IOException {
        KVIterateKeysRequest request = new KVIterateKeysRequest().setReceiverId(nodeId);
        return stores[nodeId].iterateKeys(request).getKeys();
    }

    private int randomNodeId() {
        return getRNG().nextInt(stores.length);
    }

    private int randomFollowerId(int leaderId) {
        int id = randomNodeId();
        return id != leaderId ? id : randomFollowerId(leaderId);
    }

    void assertEntriesForAll(Map<String, String> expectedEntries) throws IOException {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (int nodeId = 0; nodeId < stores.length; nodeId++) {
                assertEquals(expectedEntries.get(expKey), get(nodeId, expKey));
            }
        }
    }

    void assertStoreSizeForAll(int size) throws IOException {
        for (int nodeId = 0; nodeId < stores.length; nodeId++) {
            assertEquals(size, iterateKeys(nodeId).size());
        }
    }

    @AfterEach
    @Override
    void tearDown() throws Exception {
        super.tearDown();
        for (KVStoreRPC store : stores) {
            store.shutdown();
        }
    }
}
