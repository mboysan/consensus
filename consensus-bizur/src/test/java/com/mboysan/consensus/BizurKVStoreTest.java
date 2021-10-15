package com.mboysan.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BizurKVStoreTest extends BizurTestBase {

    BizurKVStore[] bizurStores;

    @Override
    void init(int numServers) throws Exception {
        super.init(numServers);
        bizurStores = new BizurKVStore[numServers];
        for (int i = 0; i < numServers; i++) {
            bizurStores[i] = new BizurKVStore(nodes[i]);
            bizurStores[i].start();
        }
    }

    @Test
    void testPutGet() throws Exception {
        init(5);

        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "testKey" + i;
            String val = "testVal" + i;
            assertTrue(bizurStores[randomNodeId()].put(key, val));
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
            assertTrue(bizurStores[randomNodeId()].put(key, "testVal" + i));
            assertTrue(bizurStores[randomNodeId()].remove(key));
        }
        advanceTimeForElections();  // sync
        assertStoreSizeForAll(0);
    }

    @Test
    void multiThreadTest() throws Exception {
        init(5);

        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        List<Future<?>> results = new ArrayList<>();
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            Future<?> f = exec.submit(() -> {
                String key = "testKey" + finalI;
                String val = "testVal" + finalI;
                assertTrue(bizurStores[randomNodeId()].put(key, val));
                if (getRNG().nextBoolean()) {   // in some cases, remove the entry
                    assertTrue(bizurStores[randomNodeId()].remove(key));
                } else {    // in other cases, just leave it inserted.
                    expectedEntries.put(key, val);
                }
            });
            results.add(f);
        }
        for (Future<?> result : results) {
            result.get();
        }
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }

    // fixme: this test fails sometimes, need investigation
    @Test
    void testFollowerFailure() throws Exception {
        int numServers = 5;
        init(numServers);
        int leaderId = assertOneLeader();

        int totalAllowedKills = numServers / 2;
        AtomicInteger totalKilled = new AtomicInteger(0);
        ConcurrentHashMap<Integer, Object> killedNodes = new ConcurrentHashMap<>();
        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        List<Future<?>> results = new ArrayList<>();
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            Future<?> f = exec.submit(() -> {
                String key = "testKey" + finalI;
                String val = "testVal" + finalI;

                int followerId = randomFollowerId(leaderId);
                if (getRNG().nextBoolean()) {
                    if (totalKilled.incrementAndGet() < totalAllowedKills) {
                        // try killing node
                        if (killedNodes.putIfAbsent(followerId, new Object()) == null) {
                            kill(followerId);
                            assertThrows(RuntimeException.class, () -> bizurStores[followerId].put(key, val));
                            assertTrue(bizurStores[leaderId].put(key, val)); // this shall never fail in this test
                            expectedEntries.put(key, val);
                            revive(followerId);
                            killedNodes.remove(followerId);
                        }
                        totalKilled.decrementAndGet();
                    }
                } else {
                    assertTrue(bizurStores[leaderId].put(key, val));
                    expectedEntries.put(key, val);
                }
            });
            results.add(f);
        }
        for (Future<?> result : results) {
            result.get();
        }
        advanceTimeForElections();  // sync
        assertEntriesForAll(expectedEntries);
    }

    private int randomNodeId() {
        return getRNG().nextInt(bizurStores.length);
    }

    private int randomFollowerId(int leaderId) {
        int id = randomNodeId();
        return id != leaderId ? id : randomFollowerId(leaderId);
    }

    private void assertEntriesForAll(Map<String, String> expectedEntries) {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (BizurKVStore bizurStore : bizurStores) {
                assertEquals(expectedEntries.get(expKey), bizurStore.get(expKey));
            }
        }
    }

    private void assertStoreSizeForAll(int size) {
        for (BizurKVStore bizurStore : bizurStores) {
            assertEquals(size, bizurStore.keySet().size());
        }
    }

    @AfterEach
    @Override
    void tearDown() throws Exception {
        super.tearDown();
        for (BizurKVStore bizurStore : bizurStores) {
            bizurStore.shutdown();
        }
    }

}