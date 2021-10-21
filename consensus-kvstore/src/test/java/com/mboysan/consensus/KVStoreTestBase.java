package com.mboysan.consensus;

import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class KVStoreTestBase<N extends AbstractNode<?>> extends NodeTestBase<N> {

    private KVStore[] stores;

    @Override
    void init(int numServers) throws Exception {
        super.init(numServers);
        stores = new KVStore[numServers];
        for (int i = 0; i < numServers; i++) {
            stores[i] = createKVStore(getNode(i));
            stores[i].start();
        }
    }

    abstract KVStore createKVStore(N node);

    @Test
    void testPutGet() throws Exception {
        init(5);

        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            String val = "testVal" + i;
            assertTrue(stores[randomNodeId()].put(key, val));
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
            assertTrue(stores[randomNodeId()].put(key, "testVal" + i));
            assertTrue(stores[randomNodeId()].remove(key));
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
                assertTrue(stores[randomNodeId()].put(key, val));
                if (getRNG().nextBoolean()) {   // in some cases, remove the entry
                    assertTrue(stores[randomNodeId()].remove(key));
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
                            assertThrows(KVOperationException.class, () -> stores[followerId].put(key, val));
                            assertTrue(stores[leaderId].put(key, val)); // this shall never fail in this test
                            expectedEntries.put(key, val);
                            revive(followerId);
                            killedNodes.remove(followerId);
                        }
                        totalKilled.decrementAndGet();
                    }
                } else {
                    assertTrue(stores[leaderId].put(key, val));
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }


    private int randomNodeId() {
        return getRNG().nextInt(stores.length);
    }

    private int randomFollowerId(int leaderId) {
        int id = randomNodeId();
        return id != leaderId ? id : randomFollowerId(leaderId);
    }

    void assertEntriesForAll(Map<String, String> expectedEntries) throws KVOperationException {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (KVStore store : stores) {
                assertEquals(expectedEntries.get(expKey), store.get(expKey));
            }
        }
    }

    void assertStoreSizeForAll(int size) throws KVOperationException {
        for (KVStore store : stores) {
            assertEquals(size, store.keySet().size());
        }
    }

    @AfterEach
    @Override
    void tearDown() throws Exception {
        super.tearDown();
        for (KVStore store : stores) {
            store.shutdown();
        }
    }
}
