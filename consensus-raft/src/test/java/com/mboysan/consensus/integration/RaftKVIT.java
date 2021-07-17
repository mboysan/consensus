package com.mboysan.consensus.integration;

import com.mboysan.consensus.KVStore;
import com.mboysan.consensus.NettyTransport;
import com.mboysan.consensus.RaftKVStore;
import com.mboysan.consensus.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link com.mboysan.consensus.RaftKVStore} Integration Test.
 */
public class RaftKVIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVIT.class);

    private static final int NUM_NODES = 3;

    KVStore[] stores;
    //    Transport transport;
    Map<Integer, String> destinations = Map.of(
            0, "localhost:8080",
            1, "localhost:8081",
            2, "localhost:8082"
    );
    Transport[] transports;

    @BeforeEach
    void setUp() throws Exception {
//        transport = new InVMTransport();
        transports = new Transport[NUM_NODES];

        stores = new KVStore[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            transports[i] = new NettyTransport(8080 + i, destinations);
            stores[i] = new RaftKVStore(i, transports[i]);
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
//        transport.shutdown();
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


    public static void main(String[] args) throws Exception {
        RaftKVIT raftKVIT = new RaftKVIT();
        raftKVIT.setUp();
        raftKVIT.tearDown();
    }
}
