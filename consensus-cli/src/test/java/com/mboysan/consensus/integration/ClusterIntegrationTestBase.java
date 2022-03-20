package com.mboysan.consensus.integration;

import com.mboysan.consensus.KVOperationException;
import com.mboysan.consensus.KVStoreClient;
import com.mboysan.consensus.KVStoreClusterBase;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

abstract class ClusterIntegrationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterIntegrationTestBase.class);

    void testKVOperationsSimple(KVStoreClusterBase cluster) throws KVOperationException {
        KVStoreClient client0 = cluster.getClient(0);
        KVStoreClient client1 = cluster.getClient(1);

        client0.set("k0", "v0");
        client1.set("k1", "v1");
        client0.set("toDelete", "value");
        client1.delete("toDelete");

        assertEquals("v0", client0.get("k0"));
        assertEquals("v1", client1.get("k1"));
        assertNull(client0.get("toDelete"));
    }

    void testKVOperationsSequential(KVStoreClusterBase cluster) throws KVOperationException {
        long startTime = System.currentTimeMillis();
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "k" + i;
            String val = "v" + i;
            cluster.getRandomClient().set(key, val);
            if (new SecureRandom().nextBoolean()) {   // in some cases, remove the entry
                cluster.getRandomClient().delete(key);
            } else {    // in other cases, just leave it inserted.
                expectedEntries.put(key, val);
            }
        }
        assertEntriesForAllConnectedClients(cluster, expectedEntries);
        LOGGER.info("testKVOperationsSequential exec time : {}", (System.currentTimeMillis() - startTime));
    }

    void testKVOperationsMultiThreaded(KVStoreClusterBase cluster)
            throws InterruptedException, KVOperationException, ExecutionException
    {
        long startTime = System.currentTimeMillis();
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        try (MultiThreadExecutor exec = new MultiThreadExecutor(4)) {
            for (int i = 0; i < 100; i++) {
                int finalI = i;
                exec.execute(() -> {
                    String key = "k" + finalI;
                    String val = "v" + finalI;
                    cluster.getRandomClient().set(key, val);
                    if (new SecureRandom().nextBoolean()) {   // in some cases, remove the entry
                        cluster.getRandomClient().delete(key);
                    } else {    // in other cases, just leave it inserted.
                        expectedEntries.put(key, val);
                    }
                });
            }
        }
        assertEntriesForAllConnectedClients(cluster, expectedEntries);
        LOGGER.info("testKVOperationsMultiThreaded exec time : {}", (System.currentTimeMillis() - startTime));
    }

    void testKVStoreShutdownAndStart(KVStoreClusterBase cluster)
            throws IOException, InterruptedException, KVOperationException, ExecutionException
    {
        cluster.getStore(0).shutdown();

        awaiting(KVOperationException.class, () -> cluster.getClient(1).set("k0", "v0"));

        cluster.getStore(0).start().get();

        cluster.getClient(0).set("k1", "v1");

        assertEquals("v1", cluster.getClient(0).get("k1"));
        assertEquals("v0", cluster.getClient(1).get("k0"));
    }

    private void assertEntriesForAllConnectedClients(KVStoreClusterBase cluster, Map<String, String> expectedEntries) throws KVOperationException {
        assertStoreSizeForAll(cluster, expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (KVStoreClient client : cluster.getClients()) {
                assertEquals(expectedEntries.get(expKey), client.get(expKey));
            }
        }
    }

    private void assertStoreSizeForAll(KVStoreClusterBase cluster, int size) throws KVOperationException {
        for (KVStoreClient client : cluster.getClients()) {
            assertEquals(size, client.iterateKeys().size());
        }
    }

}
