package com.mboysan.consensus.integration;

import com.mboysan.consensus.KVStore;
import com.mboysan.consensus.NettyTransport;
import com.mboysan.consensus.NettyTransportConfig;
import com.mboysan.consensus.RaftConfig;
import com.mboysan.consensus.RaftKVStore;
import com.mboysan.consensus.RaftNode;
import com.mboysan.consensus.Transport;
import org.aeonbits.owner.ConfigFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link com.mboysan.consensus.RaftKVStore} Integration Test.
 * TODO: improve this test
 */
public class RaftKVIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVIT.class);

    private static final int NUM_NODES = 3;

    KVStore[] stores;
    String destinations = "0=localhost:8080, 1=localhost:8081, 2=localhost:8082";
    Transport[] transports;

    @BeforeEach
    void setUp() throws Exception {
        transports = new Transport[NUM_NODES];

        stores = new KVStore[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            transports[i] = createTransport(8080 + i, destinations);
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

    NettyTransport createTransport(int port, String destinations) {
        Properties properties = new Properties();
        properties.put("transport.netty.port", port + "");
        properties.put("transport.netty.destinations", destinations);
        // create new config per transport
        NettyTransportConfig config = ConfigFactory.create(NettyTransportConfig.class, properties);
        return new NettyTransport(config);
    }

    RaftNode createNode(int nodeId, Transport transport) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        // create new config per node (for unique ids)
        RaftConfig config = ConfigFactory.create(RaftConfig.class, properties);
        return new RaftNode(config, transport);
    }
}
