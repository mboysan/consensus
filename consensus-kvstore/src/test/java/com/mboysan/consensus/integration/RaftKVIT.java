package com.mboysan.consensus.integration;

import com.mboysan.consensus.KVOperationException;
import com.mboysan.consensus.KVStore;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RaftKVStore} Integration Test.
 * TODO: improve this test
 */
public class RaftKVIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVIT.class);

    private static final Random RNG = new Random();

    private static final int NUM_NODES = 3;

    KVStore[] stores;
    String destinations = "0=localhost:8080, 1=localhost:8081, 2=localhost:8082";
    Transport[] transports;

    @BeforeEach
    void setUp() throws Exception {
        transports = new Transport[NUM_NODES];

        stores = new KVStore[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            transports[i] = createServerTransport(8080 + i, destinations);
            RaftNode raftNode = createNode(i, transports[i]);
            stores[i] = new RaftKVStore(raftNode);
        }
        Thread[] threads = new Thread[stores.length];
        for (int i = 0; i < NUM_NODES; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    stores[finalI].start();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (KVStore store : stores) {
            store.shutdown();
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
                assertTrue(stores[randomNodeId()].put(key, val));
                if (RNG.nextBoolean()) {   // in some cases, remove the entry
                    assertTrue(stores[randomNodeId()].remove(key));
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

        stores[1].put("k0", "v0");
        Thread.sleep(5000);

        System.out.println("RESTART++++++++++++++++++++++++++++");
        stores[0].start();

        Thread.sleep(5000);
        assertEquals("v0", stores[0].get("k0"));
        assertEquals("v0", stores[1].get("k0"));
    }

    private int randomNodeId() {
        return RNG.nextInt(stores.length);
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

    NettyServerTransport createServerTransport(int port, String destinations) {
        Properties properties = new Properties();
        properties.put("transport.netty.port", port + "");
        properties.put("transport.netty.destinations", destinations);
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyServerTransport(config);
    }

    RaftNode createNode(int nodeId, Transport transport) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        // create new config per node (for unique ids)
        RaftConfig config = Configuration.newInstance(RaftConfig.class, properties);
        return new RaftNode(config, transport);
    }
}
