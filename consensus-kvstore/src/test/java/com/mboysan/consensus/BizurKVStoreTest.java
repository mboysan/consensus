package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
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

class BizurKVStoreTest extends KVStoreTestBase {

    private InVMTransport nodeServingTransport;
    private BizurKVStore[] stores;
    private KVStoreClient[] clients;
    private boolean skipTeardown = false;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

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
        properties.put(NodeConfig.Param.NODE_ID, nodeId + "");
        properties.put(BizurConfig.Param.NUM_PEERS, numPeers + "");
        properties.put(BizurConfig.Param.NUM_BUCKETS, numBuckets + "");
        properties.put(BizurConfig.Param.UPDATE_INTERVAL_MS, 50 + "");
        return CoreConfig.newInstance(BizurConfig.class, properties);
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

    @Test
    void testFailResponses() throws Exception {
        skipTeardown = true;

        BizurNode node = Mockito.mock(BizurNode.class);
        Mockito.when(node.get(Mockito.any())).thenThrow(new IOException());
        Mockito.when(node.set(Mockito.any())).thenThrow(new IOException());
        Mockito.when(node.delete(Mockito.any())).thenThrow(new IOException());
        Mockito.when(node.iterateKeys(Mockito.any())).thenThrow(new IOException());
        Mockito.when(node.customRequest(Mockito.any())).thenThrow(new IOException());
        BizurKVStore store = new BizurKVStore(node, null);

        testFailedResponses(store);
    }

    @Test
    void testDumpMetrics() throws Exception {
        this.initCluster(5, 50);
        super.dumpStoreMetricsTest();
    }
}
