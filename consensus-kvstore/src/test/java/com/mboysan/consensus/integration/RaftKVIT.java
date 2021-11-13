package com.mboysan.consensus.integration;

import com.mboysan.consensus.KVOperationException;
import com.mboysan.consensus.KVStoreClient;
import com.mboysan.consensus.KVStoreRPC;
import com.mboysan.consensus.NettyClientTransport;
import com.mboysan.consensus.NettyServerTransport;
import com.mboysan.consensus.RaftKVStore;
import com.mboysan.consensus.RaftNode;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RaftKVStore} Integration Test.
 * TODO: improve this test
 */
public class RaftKVIT {
    private static final Random RNG = new Random();

    private static final int NUM_NODES = 3;
    private static final Map<Integer, String> DESTINATIONS = new HashMap<>() {{
        put(0, "localhost:8080");
        put(1, "localhost:8081");
        put(2, "localhost:8082");
    }};

    private KVStoreRPC[] stores;
    private KVStoreClient[] clients;

    @BeforeEach
    void setUp() throws Exception {
        startServers();
        startClients();
    }

    private void startServers() throws ExecutionException, InterruptedException, IOException {
        List<Future<?>> startFutures = new ArrayList<>();
        stores = new KVStoreRPC[NUM_NODES];
        for (int i = 0; i < stores.length; i++) {
            Transport transport = createServerTransport(8080 + i);
            RaftNode raftNode = createNode(i, transport);
            stores[i] = new RaftKVStore(raftNode);
            startFutures.add(stores[i].start());
        }
        for (Future<?> startFuture : startFutures) {
            startFuture.get();
        }
    }

    private void startClients() throws IOException {
        clients = new KVStoreClient[NUM_NODES];
        for (int i = 0; i < clients.length; i++) {
            Transport transport = createClientTransport(i);
            clients[i] = new KVStoreClient(transport);
            clients[i].start();
        }
    }

    @AfterEach
    void tearDown() {
        for (KVStoreRPC store : stores) {
            store.shutdown();
        }
        for (KVStoreClient client : clients) {
            client.shutdown();
        }
    }

    @Test
    void testMultiPutGetMultiThreaded() throws KVOperationException, InterruptedException, ExecutionException {
        Map<String, String> expectedEntries = new ConcurrentHashMap<>();
        MultiThreadExecutor exec = new MultiThreadExecutor();
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            exec.execute(() -> {
                String key = "k" + finalI;
                String val = "v" + finalI;
                clients[randomNodeId()].set(key, val);
                if (RNG.nextBoolean()) {   // in some cases, remove the entry
                    clients[randomNodeId()].delete(key);
                } else {    // in other cases, just leave it inserted.
                    expectedEntries.put(key, val);
                }
            });
        }
        exec.endExecution();
        Thread.sleep(5000);
        assertEntriesForAll(expectedEntries);
    }

    @Test
    void testShutdownAndStart() throws Exception {
        stores[0].shutdown();
        System.out.println("SHUTDOWN------------------------------");
        Thread.sleep(20000);

        clients[1].set("k0", "v0");
        Thread.sleep(5000);

        System.out.println("RESTART++++++++++++++++++++++++++++");
        stores[0].start();

        Thread.sleep(5000);
        assertEquals("v0", clients[0].get("k0"));
        assertEquals("v0", clients[1].get("k0"));
    }

    private int randomNodeId() {
        return RNG.nextInt(stores.length);
    }

    void assertEntriesForAll(Map<String, String> expectedEntries) throws KVOperationException {
        assertStoreSizeForAll(expectedEntries.size());
        for (String expKey : expectedEntries.keySet()) {
            for (KVStoreClient client : clients) {
                assertEquals(expectedEntries.get(expKey), client.get(expKey));
            }
        }
    }

    void assertStoreSizeForAll(int size) throws KVOperationException {
        for (KVStoreClient client : clients) {
            assertEquals(size, client.iterateKeys().size());
        }
    }

    NettyServerTransport createServerTransport(int port) {
        StringJoiner sj = new StringJoiner(",");
        for (Map.Entry<Integer, String> entry : DESTINATIONS.entrySet()) {
            sj.add(entry.getKey() + "=" + entry.getValue());
        }
        String destinations = sj.toString();

        Properties properties = new Properties();
        properties.put("transport.netty.port", port + "");
        properties.put("transport.netty.destinations", destinations);
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyServerTransport(config);
    }

    NettyClientTransport createClientTransport(int nodeId) {
        String destination = nodeId + "=" + DESTINATIONS.get(nodeId);

        Properties properties = new Properties();
        properties.put("transport.netty.destinations", destination);
        properties.put("transport.message.callbackTimeoutMs", "1000000");   // lets make the callback time very large
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyClientTransport(config);
    }

    RaftNode createNode(int nodeId, Transport transport) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        // create new config per node (for unique ids)
        RaftConfig config = Configuration.newInstance(RaftConfig.class, properties);
        return new RaftNode(config, transport);
    }
}
