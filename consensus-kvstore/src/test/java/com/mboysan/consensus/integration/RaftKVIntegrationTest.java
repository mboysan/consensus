package com.mboysan.consensus.integration;

import com.mboysan.consensus.*;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.util.NettyUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RaftKVStore} Integration Test.
 * TODO: improve this test
 */
public class RaftKVIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVIntegrationTest.class);

    private static final String HOST_NAME = "localhost";

    private static final Random RNG = new Random();

    private static final List<Destination> NODE_DESTINATIONS = new ArrayList<>();
    static {
        addDestination(NODE_DESTINATIONS, 0, NettyUtil.findFreePort());
        addDestination(NODE_DESTINATIONS, 1, NettyUtil.findFreePort());
        addDestination(NODE_DESTINATIONS, 2, NettyUtil.findFreePort());
        addDestination(NODE_DESTINATIONS, 3, NettyUtil.findFreePort());
        addDestination(NODE_DESTINATIONS, 4, NettyUtil.findFreePort());
    }
    private static final String NODE_DESTINATIONS_STR = NettyUtil.convertDestinationsListToProps(NODE_DESTINATIONS);

    private static final List<Destination> STORE_DESTINATIONS = new ArrayList<>();
    static {
        addDestination(STORE_DESTINATIONS, 0, NettyUtil.findFreePort());
        addDestination(STORE_DESTINATIONS, 1, NettyUtil.findFreePort());
        addDestination(STORE_DESTINATIONS, 2, NettyUtil.findFreePort());
        addDestination(STORE_DESTINATIONS, 3, NettyUtil.findFreePort());
        addDestination(STORE_DESTINATIONS, 4, NettyUtil.findFreePort());
    }

    private KVStoreRPC[] stores;
    private KVStoreClient[] clients;

    @BeforeEach
    void setUp() throws Exception {
        startServers();
        startClients();
    }

    private void startServers() throws ExecutionException, InterruptedException, IOException {
        List<Future<?>> startFutures = new ArrayList<>();
        stores = new KVStoreRPC[NODE_DESTINATIONS.size()];
        for (int i = 0; i < stores.length; i++) {
            Destination nodeDestination = NODE_DESTINATIONS.get(i);
            Transport nodeTransport = createServerTransport(nodeDestination);
            RaftNode raftNode = createNode(i, nodeTransport);

            Destination storeDestination = STORE_DESTINATIONS.get(i);
            Transport storeTransport = createServerTransport(storeDestination);
            stores[i] = new RaftKVStore(raftNode, storeTransport);
            startFutures.add(stores[i].start());
        }
        for (Future<?> startFuture : startFutures) {
            startFuture.get();
        }
    }

    private void startClients() throws IOException {
        clients = new KVStoreClient[STORE_DESTINATIONS.size()];
        for (int i = 0; i < clients.length; i++) {
            Destination storeDestination = STORE_DESTINATIONS.get(i);
            Transport transport = createClientTransport(storeDestination);
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
    void testPutGetDeleteMultiThreaded() throws KVOperationException, InterruptedException, ExecutionException {
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
        Thread.sleep(5000); // allow time to sync
        assertEntriesForAll(expectedEntries);
    }

    /**
     * Tests the scenario in which the leader is shutdown and is restarted after a while.
     * This test might take several minutes to complete.
     */
    @Test
    void testShutdownAndStart() throws Exception {
        stores[0].shutdown();
        System.out.println("SHUTDOWN------------------------------");
        LOGGER.info("SHUTDOWN------------------------------");
        Thread.sleep(20000);

        clients[1].set("k0", "v0");
        Thread.sleep(5000);

        System.out.println("RESTART++++++++++++++++++++++++++++");
        LOGGER.info("RESTART++++++++++++++++++++++++++++");
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

    NettyServerTransport createServerTransport(Destination myAddress) {
        Properties properties = new Properties();
        properties.put("transport.netty.port", myAddress.getPort() + "");
        properties.put("transport.netty.destinations", NODE_DESTINATIONS_STR);
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyServerTransport(config);
    }

    NettyClientTransport createClientTransport(Destination storeDestination) {
        Properties properties = new Properties();
        properties.put("transport.netty.destinations", storeDestination.toString());
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

    private static void addDestination(List<Destination> destinations, int nodeId, int port) {
        Objects.requireNonNull(destinations).add(new Destination(nodeId, HOST_NAME, port));
    }
}
