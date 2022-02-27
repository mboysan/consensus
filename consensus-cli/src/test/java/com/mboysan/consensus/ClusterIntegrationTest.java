package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedRunnable;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ClusterIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterIntegrationTest.class);

    private static final Random RNG = new Random();

    @BeforeAll
    static void beforeAll() {
        KVStoreClientCLI.testingInProgress = true;
    }

    @AfterEach
    void cleanup() {
        KVStoreClientCLI.CLIENT_REFERENCES.forEach((i, client) -> client.shutdown());
        NodeCLI.NODE_REFERENCES.forEach((i, node) -> node.shutdown());
        KVStoreServerCLI.STORE_REFERENCES.forEach((i, store) -> store.shutdown());
    }

    @Test
    void testRaftKVOperationsSimple() throws IOException, InterruptedException, KVOperationException {
        testKVOperationsSimple("raft");
    }

    @Test
    void testRaftKVOperationsMultiThreaded()
            throws InterruptedException, KVOperationException, ExecutionException, IOException
    {
        testKVOperationsMultiThreaded("raft");
    }

    @Test
    void testBizurKVOperationsSimple() throws IOException, InterruptedException, KVOperationException {
        testKVOperationsSimple("bizur");
    }

    @Test
    void testBizurKVOperationsMultiThreaded()
            throws InterruptedException, KVOperationException, ExecutionException, IOException
    {
        testKVOperationsMultiThreaded("bizur");
    }

    /**
     * Setups a cluster of 5 nodes, in which 2 of them will
     * @param protocol protocol to bootstrap the cluster with
     */
    private void bootstrapCluster(String protocol) throws InterruptedException, IOException {
        List<Thread> threads = new ArrayList<>();

        threads.add(exec(() -> {
            // KV Store server with node-0 for client-0 to connect
            KVStoreServerCLI.main(new String[]{
                    "node.id=0",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.ports=8080,9090",  // nodes will connect to first port and client to second
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082,3-localhost:8083,4-localhost:8084"
            });
        }));

        threads.add(exec(() -> {
            // KV Store server with node-1 for client-1 to connect
            KVStoreServerCLI.main(new String[]{
                    "node.id=1",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.ports=8081,9091",  // nodes will connect to first port and client to second
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082,3-localhost:8083,4-localhost:8084"
            });
        }));

        threads.add(exec(() -> {
            // node-2
            NodeCLI.main(new String[]{
                    "node.id=2",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8082",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082,3-localhost:8083,4-localhost:8084"
            });
        }));

        threads.add(exec(() -> {
            // node-3
            NodeCLI.main(new String[]{
                    "node.id=3",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8083",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082,3-localhost:8083,4-localhost:8084"
            });
        }));

        threads.add(exec(() -> {
            // node-4
            NodeCLI.main(new String[]{
                    "node.id=4",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8084",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082,3-localhost:8083,4-localhost:8084"
            });
        }));

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        LOGGER.info("all nodes started");

        // client-0
        KVStoreClientCLI.main(new String[]{
                "client.id=0",
                "transport.netty.destinations=0-localhost:9090"
        });

        // client-1
        KVStoreClientCLI.main(new String[]{
                "client.id=1",
                "transport.netty.destinations=0-localhost:9091"
        });
    }

    private void testKVOperationsSimple(String protocol)
            throws IOException, InterruptedException, KVOperationException
    {
        bootstrapCluster(protocol);

        KVStoreClient client0 = getClient(0);
        KVStoreClient client1 = getClient(1);

        client0.set("k0", "v0");
        client1.set("k1", "v1");
        client0.set("toDelete", "value");
        client1.delete("toDelete");

        assertEquals("v0", client0.get("k0"));
        assertEquals("v1", client1.get("k1"));
        assertNull(client0.get("toDelete"));
    }

    private void testKVOperationsMultiThreaded(String protocol)
            throws InterruptedException, KVOperationException, ExecutionException, IOException {
        bootstrapCluster(protocol);

        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        MultiThreadExecutor exec = new MultiThreadExecutor();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            exec.execute(() -> {
                String key = "k" + finalI;
                String val = "v" + finalI;
                getClient(randomClientId()).set(key, val);
                if (RNG.nextBoolean()) {   // in some cases, remove the entry
                    getClient(randomClientId()).delete(key);
                } else {    // in other cases, just leave it inserted.
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        assertEntriesForAllConnectedClients(expectedEntries);
    }

    private void testShutdownAndStart() throws Exception {
        // TODO: implement this with event dispatches
    }

    private void assertEntriesForAllConnectedClients(Map<String, String> expectedEntries) throws KVOperationException {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (KVStoreClient client : getClients()) {
                assertEquals(expectedEntries.get(expKey), client.get(expKey));
            }
        }
    }

    private void assertStoreSizeForAll(int size) throws KVOperationException {
        for (KVStoreClient client : getClients()) {
            assertEquals(size, client.iterateKeys().size());
        }
    }

    private AbstractKVStore<?> getStore(int nodeId) {
        return KVStoreServerCLI.STORE_REFERENCES.get(nodeId);
    }

    private AbstractNode<?> getNode(int nodeId) {
        return NodeCLI.NODE_REFERENCES.get(nodeId);
    }

    private KVStoreClient getClient(int clientId) {
        return KVStoreClientCLI.CLIENT_REFERENCES.get(clientId);
    }

    private Collection<KVStoreClient> getClients() {
        return KVStoreClientCLI.CLIENT_REFERENCES.values();
    }

    private int randomClientId() {
        return RNG.nextInt(getClients().size());
    }

    private static Thread exec(CheckedRunnable<Exception> runnable) {
        return new Thread(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
