package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SimKVStoreTest extends KVStoreTestBase {

    private InVMTransport nodeServingTransport;
    private SimKVStore[] stores;
    private KVStoreClient[] clients;

    private boolean skipTeardown = false;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    void initCluster(int numNodes) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        this.nodeServingTransport = new InVMTransport();
        this.stores = new SimKVStore[numNodes];
        this.clients = new KVStoreClient[numNodes];
        for (int i = 0; i < stores.length; i++) {
            SimNode node = new SimNode(simConfig(i), nodeServingTransport);
            InVMTransport clientServingTransport = new InVMTransport(i);
            stores[i] = new SimKVStore(node, clientServingTransport);
            futures.add(stores[i].start());

            clients[i] = new KVStoreClient(clientServingTransport);
            clients[i].start();
        }
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private SimConfig simConfig(int nodeId) {
        Properties properties = new Properties();
        properties.put(NodeConfig.Param.NODE_ID, nodeId + "");
        return CoreConfig.newInstance(SimConfig.class, properties);
    }

    @AfterEach
    void tearDown() {
        if (skipTeardown) {
            return;
        }
        Arrays.stream(stores).forEach(KVStoreRPC::shutdown);
        Arrays.stream(clients).forEach(KVStoreClient::shutdown);
    }

    @Override
    KVStoreClient[] getClients() {
        return clients;
    }

    @Override
    SimKVStore[] getStores() {
        return stores;
    }

    @Override
    InVMTransport getNodeServingTransport() {
        return nodeServingTransport;
    }

    @Override
    InVMTransport getClientServingTransport(int storeId) {
        return (InVMTransport) clients[storeId].getTransport();
    }

    @Test
    void testAllOperations() throws Exception {
        this.initCluster(3);
        assertDoesNotThrow(() -> getRandomClient().set("a", "b"));
        assertDoesNotThrow(() -> getRandomClient().get("a"));
        assertDoesNotThrow(() -> getRandomClient().delete("a"));
        assertDoesNotThrow(() -> getRandomClient().iterateKeys());
        assertDoesNotThrow(() -> getRandomClient().checkIntegrity(1, -1));
        assertDoesNotThrow(() -> getRandomClient().checkIntegrity(1, 1));
    }

    @Test
    void testFailResponses() throws Exception {
        skipTeardown = true;

        SimNode node = Mockito.mock(SimNode.class);
        Mockito.when(node.simulate(Mockito.any())).thenThrow(new IOException());
        Mockito.when(node.customRequest(Mockito.any())).thenThrow(new IOException());
        SimKVStore store = new SimKVStore(node, null);

        testFailedResponses(store);
    }
}
