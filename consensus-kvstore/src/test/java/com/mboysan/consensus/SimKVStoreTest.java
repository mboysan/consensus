package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.SimConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class SimKVStoreTest extends KVStoreTestBase {

    private InVMTransport nodeServingTransport;
    private SimKVStore[] stores;
    private KVStoreClient[] clients;

    private boolean skipTeardown = false;

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
        properties.put("node.id", nodeId + "");
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
        getRandomClient().set("a", "b");
        getRandomClient().get("a");
        getRandomClient().delete("a");
        getRandomClient().iterateKeys();
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
