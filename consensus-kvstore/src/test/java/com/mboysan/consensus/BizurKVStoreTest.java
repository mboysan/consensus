package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class BizurKVStoreTest extends KVStoreTestBase {

    private InVMTransport nodeServingTransport;
    private BizurKVStore[] stores;
    private KVStoreClient[] clients;

    void initCluster(int numNodes, int numBuckets) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        this.nodeServingTransport = new InVMTransport();
        this.stores = new BizurKVStore[numNodes];
        this.clients = new KVStoreClient[numNodes];
        for (int i = 0; i < stores.length; i++) {
            BizurNode node = new BizurNode(bizurConfig(i, numNodes, numBuckets), nodeServingTransport);
            InVMTransport clientServingTransport = new InVMTransport(i);
            stores[i] = new BizurKVStore(node, clientServingTransport);
            futures.add(stores[i].start());

            clients[i] = new KVStoreClient(clientServingTransport);
            clients[i].start();
        }
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private BizurConfig bizurConfig(int nodeId, int numPeers, int numBuckets) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        properties.put("bizur.numPeers", numPeers + "");
        properties.put("bizur.numBuckets", numBuckets + "");
        properties.put("bizur.updateIntervalMs", 50 + "");
        return CoreConfig.newInstance(BizurConfig.class, properties);
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
    BizurKVStore[] getStores() {
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
    void testPutGetSequentialWithSingleBucket() throws Exception {
        this.initCluster(5, 1);
        super.putGetSequentialTest();
    }

    @Test
    void testPutGetSequentialWithMultiBucket() throws Exception {
        this.initCluster(5, 100);
        super.putGetSequentialTest();
    }

    @Test
    void testDeleteSequentialWithSingleBucket() throws Exception {
        this.initCluster(5, 1);
        super.deleteSequentialTest();
    }

    @Test
    void testDeleteSequentialWithMultiBucket() throws Exception {
        this.initCluster(5, 100);
        super.deleteSequentialTest();
    }

    @Test
    void testMultiThreadedWithSingleBucket() throws Exception {
        this.initCluster(5, 1);
        super.multiThreadTest();
    }

    @Test
    void testMultiThreadedWithMultiBucket() throws Exception {
        this.initCluster(5, 100);
        super.multiThreadTest();
    }

    @Test
    void testStoreFailureSequentialWithSingleBucket() throws Exception {
        this.initCluster(5, 1);
        super.storeFailureSequentialTest();
    }

    @Test
    void testStoreFailureSequentialWithMultiBucket() throws Exception {
        this.initCluster(5, 100);
        super.storeFailureSequentialTest();
    }
}
