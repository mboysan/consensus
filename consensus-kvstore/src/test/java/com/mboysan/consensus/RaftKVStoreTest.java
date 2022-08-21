package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class RaftKVStoreTest extends KVStoreTestBase {

    private InVMTransport nodeServingTransport;
    private RaftKVStore[] stores;
    private KVStoreClient[] clients;

    void initCluster(int numNodes) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        this.nodeServingTransport = new InVMTransport();
        this.stores = new RaftKVStore[numNodes];
        this.clients = new KVStoreClient[numNodes];
        for (int i = 0; i < stores.length; i++) {
            RaftNode node = new RaftNode(raftConfig(i), nodeServingTransport);
            InVMTransport clientServingTransport = new InVMTransport(i);
            stores[i] = new RaftKVStore(node, clientServingTransport);
            futures.add(stores[i].start());

            clients[i] = new KVStoreClient(clientServingTransport);
            clients[i].start();
        }
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private RaftConfig raftConfig(int nodeId) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        properties.put("raft.updateIntervalMs", 50 + "");
        properties.put("raft.electionTimeoutMs", 100 + "");
        return CoreConfig.newInstance(RaftConfig.class, properties);
    }

    @AfterEach
    void tearDown() {
        Arrays.stream(stores).forEach(KVStoreRPC::shutdown);
        Arrays.stream(clients).forEach(KVStoreClient::shutdown);
    }

    @Override
    KVStoreClient[] getClients() {
        return clients;
    }

    @Override
    RaftKVStore[] getStores() {
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
    void testGetSetSequential() throws Exception {
        this.initCluster(5);
        super.putGetSequentialTest();
    }

    @Test
    void testDeleteSequential() throws Exception {
        this.initCluster(5);
        super.deleteSequentialTest();
    }

    @Test
    void testMultiThreaded() throws Exception {
        this.initCluster(5);
        super.multiThreadTest();
    }

    @Test
    void testStoreFailureSequential() throws Exception {
        this.initCluster(5);
        super.storeFailureSequentialTest();
    }

}
