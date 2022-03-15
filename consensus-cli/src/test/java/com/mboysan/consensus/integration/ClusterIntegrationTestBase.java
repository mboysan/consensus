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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ClusterIntegrationTestBase {

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

    void testKVOperationsMultiThreaded(KVStoreClusterBase cluster)
            throws InterruptedException, KVOperationException, ExecutionException
    {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        MultiThreadExecutor exec = new MultiThreadExecutor();
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            exec.execute(() -> {
                String key = "k" + finalI;
                String val = "v" + finalI;
                cluster.getClient(cluster.randomClientId()).set(key, val);
                if (new SecureRandom().nextBoolean()) {   // in some cases, remove the entry
                    cluster.getClient(cluster.randomClientId()).delete(key);
                } else {    // in other cases, just leave it inserted.
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        assertEntriesForAllConnectedClients(cluster, expectedEntries);
    }

    void testKVStoreShutdownAndStart(KVStoreClusterBase cluster)
            throws IOException, InterruptedException, KVOperationException, ExecutionException
    {
        cluster.getStore(0).shutdown();

        setRetrying(cluster, 1, "k0", "v0");

        cluster.getStore(0).start().get();

        cluster.getClient(0).set("k1", "v1");

        assertEquals("v1", cluster.getClient(0).get("k1"));
        assertEquals("v0", cluster.getClient(1).get("k0"));
    }

    private void setRetrying(KVStoreClusterBase cluster, int clientId, String key, String value)
            throws InterruptedException
    {
        int retries = 5;
        for (int i = 0; i < retries; i++) {
            try {
                cluster.getClient(clientId).set(key, value);
                return;
            } catch (KVOperationException e) {
                LOGGER.info("set failed, waiting for new election. received error={}", e.getMessage());
                Thread.sleep(5000);
            }
        }
        fail("set by client-%d failed after %d retries".formatted(clientId, retries));
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
